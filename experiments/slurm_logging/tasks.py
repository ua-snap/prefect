"""Prefect tasks for the SLURM logging experiment."""

from __future__ import annotations

import logging

import paramiko
from prefect import task
from prefect.logging import get_run_logger

from experiments.slurm_logging import imported_logging_demo
from time import sleep

from experiments.slurm_logging.helpers import (
    parse_sacct_job_state,
    parse_sbatch_job_id,
    resolve_remote_experiment_dir,
    slurm_log_paths,
    upload_experiment_scripts,
)
from utils import utils

FAIL_MODES = ("none", "exit", "timeout", "exception")


@task
def deploy_experiment_scripts(ssh: paramiko.SSHClient, remote_experiment_dir: str) -> str:
    """Upload worker.py and job.slurm to the remote HPC."""
    logger = get_run_logger()
    exp_dir = resolve_remote_experiment_dir(ssh, remote_experiment_dir)
    logger.info(f"Deploying experiment scripts to {exp_dir}")
    upload_experiment_scripts(ssh, exp_dir)
    logger.info("Experiment scripts deployed")
    return exp_dir


@task
def demo_orchestrator_logging() -> None:
    """Exercise logging channels on the Prefect agent (orchestrator side)."""
    logger = get_run_logger()
    print("[PREFECT-TASK] print with log_prints inherited from flow")

    logger.info("[PREFECT-TASK] get_run_logger.info")
    logger.warning("[PREFECT-TASK] get_run_logger.warning")

    stdlib_logger = logging.getLogger("orchestrator_stdlib")
    stdlib_logger.info("[PREFECT-TASK] stdlib logging (may not reach Prefect UI)")

    imported_logging_demo.log_from_imported_module()
    imported_logging_demo.log_with_prefect_logger()


@task
def submit_slurm_job(
    ssh: paramiko.SSHClient,
    remote_experiment_dir: str,
    fail_mode: str = "none",
) -> str:
    """Submit job.slurm on the remote HPC and return the SLURM job ID."""
    logger = get_run_logger()

    if fail_mode not in FAIL_MODES:
        raise ValueError(f"fail_mode must be one of {FAIL_MODES}, got {fail_mode!r}")

    time_limit = "00:00:30" if fail_mode == "timeout" else "00:02:00"
    exp_dir = resolve_remote_experiment_dir(ssh, remote_experiment_dir)
    cmd = (
        f"mkdir -p {exp_dir}/logs && "
        f"sbatch --partition=t2small "
        f"--time={time_limit} "
        f'--chdir="{exp_dir}" '
        f'--output="{exp_dir}/logs/slurm-%j.out" '
        f'--error="{exp_dir}/logs/slurm-%j.err" '
        f'"{exp_dir}/job.slurm" {fail_mode}'
    )

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
    logger.info(f"sbatch stdout: {stdout!r}")
    if stderr:
        logger.warning(f"sbatch stderr: {stderr!r}")

    if exit_status != 0:
        raise RuntimeError(f"sbatch failed (exit {exit_status}): {stderr or stdout}")

    job_id = parse_sbatch_job_id(stdout)
    logger.info(f"Submitted SLURM job {job_id} (fail_mode={fail_mode})")
    return job_id


@task
def wait_for_slurm_job(
    ssh: paramiko.SSHClient,
    job_id: str,
    *,
    fail_close: bool = True,
    poll_interval: int = 10,
) -> dict[str, str]:
    """Poll squeue until the job finishes, optionally validating with sacct."""
    logger = get_run_logger()
    logger.info(f"Waiting for job {job_id} (fail_close={fail_close})")

    while True:
        exit_status, stdout, stderr = utils.exec_command(ssh, f"squeue -h -j {job_id}")
        if exit_status != 0:
            raise RuntimeError(f"squeue failed for job {job_id}: {stderr}")

        if not stdout.strip():
            break

        sleep(poll_interval)

    logger.info(f"Job {job_id} left the queue; checking sacct")

    cmd = f"sacct -j {job_id} --format=JobID,State,ExitCode -P -n"
    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    result = {"job_id": job_id, "state": "UNKNOWN", "exit_code": "UNKNOWN"}

    if exit_status != 0 or not stdout.strip():
        message = f"sacct unavailable for job {job_id}: {stderr or stdout}"
        if fail_close:
            raise RuntimeError(message)
        logger.warning(f"[fail-open] {message}")
        return result

    try:
        succeeded, state, exit_code = parse_sacct_job_state(stdout, job_id)
    except ValueError as exc:
        if fail_close:
            raise RuntimeError(str(exc)) from exc
        logger.warning(f"[fail-open] {exc}")
        return result

    result.update({"state": state, "exit_code": exit_code, "succeeded": succeeded})
    logger.info(f"sacct: job {job_id} state={state} exit_code={exit_code}")

    if not succeeded:
        message = f"SLURM job {job_id} failed: state={state} exit_code={exit_code}"
        if fail_close:
            raise RuntimeError(message)
        logger.warning(f"[fail-open] {message}")

    return result


@task
def fetch_slurm_logs(
    ssh: paramiko.SSHClient,
    job_id: str,
    remote_experiment_dir: str,
    *,
    tail_lines: int = 200,
) -> dict[str, str]:
    """Fetch SLURM .out/.err files from the remote HPC into Prefect logs."""
    logger = get_run_logger()
    exp_dir = resolve_remote_experiment_dir(ssh, remote_experiment_dir)
    paths = slurm_log_paths(exp_dir, job_id)
    contents: dict[str, str] = {}

    for label, path in paths.items():
        cmd = f"test -f {path} && tail -n {tail_lines} {path} || echo '(file missing: {path})'"
        exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
        if exit_status != 0:
            logger.warning(f"Could not read {path}: {stderr}")
            contents[label] = stderr or stdout
        else:
            contents[label] = stdout
            logger.info(f"--- slurm-{job_id}.{label} (last {tail_lines} lines) ---\n{stdout}")

    return contents
