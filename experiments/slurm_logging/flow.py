"""Prefect flow for experimenting with SLURM job observability."""

from __future__ import annotations

import paramiko
from prefect import flow
from prefect.logging import get_run_logger

from experiments.slurm_logging import tasks
from utils import utils

DEFAULT_SSH_HOST = "chinook04.rcs.alaska.edu"
DEFAULT_SSH_PORT = 22


@flow(log_prints=True)
def slurm_logging_experiment(
    ssh_username: str,
    ssh_private_key_path: str,
    remote_experiment_dir: str = "~/prefect_slurm_logging_experiment",
    *,
    ssh_host: str = DEFAULT_SSH_HOST,
    ssh_port: int = DEFAULT_SSH_PORT,
    fail_mode: str = "none",
    fail_close: bool = True,
    fetch_logs: bool = True,
    run_sync_comparison: bool = False,
    partition: str | None = None,
    skip_deploy: bool = False,
) -> dict:
    """Run a SLURM logging experiment and observe what reaches Prefect UI.

    Parameters
    ----------
    ssh_username:
        HPC SSH username.
    ssh_private_key_path:
        Path to SSH private key for key-based auth.
    remote_experiment_dir:
        Directory on the HPC where worker.py and job.slurm are deployed.
    fail_mode:
        One of none, exit, timeout, exception — passed to the dummy worker.
    fail_close:
        If True, raise when sacct reports job failure. If False, only log warnings.
    fetch_logs:
        If True, tail SLURM .out/.err into Prefect logs after the job finishes.
    run_sync_comparison:
        If True, also run the worker via blocking srun (streams through SSH).
    partition:
        Optional SLURM partition for sbatch/srun.
    skip_deploy:
        If True, assume scripts are already on the HPC at remote_experiment_dir.
    """
    logger = get_run_logger()
    print("[FLOW] print with log_prints=True on flow")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    result: dict = {
        "fail_mode": fail_mode,
        "fail_close": fail_close,
        "remote_experiment_dir": remote_experiment_dir,
        "job_id": None,
        "slurm_logs": {},
        "sync_srun": None,
    }

    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)
        utils.ensure_slurm(ssh)

        tasks.demo_orchestrator_logging()

        if not skip_deploy:
            remote_experiment_dir = tasks.deploy_experiment_scripts(
                ssh, remote_experiment_dir
            )

        if run_sync_comparison:
            exit_status, stdout, stderr = tasks.run_synchronous_srun(
                ssh,
                remote_experiment_dir,
                fail_mode=fail_mode,
                partition=partition,
            )
            result["sync_srun"] = {
                "exit_status": exit_status,
                "stdout": stdout,
                "stderr": stderr,
            }

        job_id = tasks.submit_slurm_job(
            ssh,
            remote_experiment_dir,
            fail_mode=fail_mode,
            partition=partition,
        )
        result["job_id"] = job_id

        sacct_result = tasks.wait_for_slurm_job(ssh, job_id, fail_close=fail_close)
        result["sacct"] = sacct_result

        if fetch_logs:
            result["slurm_logs"] = tasks.fetch_slurm_logs(
                ssh,
                job_id,
                remote_experiment_dir,
            )

        logger.info(f"Experiment complete: {result}")
        return result

    finally:
        ssh.close()


if __name__ == "__main__":
    slurm_logging_experiment(
        ssh_username="YOUR_USERNAME",
        ssh_private_key_path="~/.ssh/id_rsa",
        fail_mode="none",
        fail_close=True,
        fetch_logs=True,
        run_sync_comparison=False,
    )
