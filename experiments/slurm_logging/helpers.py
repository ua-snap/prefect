"""Import-safe helpers for the SLURM logging experiment."""

from __future__ import annotations

import re
from pathlib import Path

import paramiko

LOCAL_EXPERIMENT_DIR = Path(__file__).resolve().parent
REMOTE_SCRIPT_NAMES = ("worker.py", "job.slurm")


def parse_sbatch_job_id(sbatch_stdout: str) -> str:
    """Parse a job ID from sbatch stdout (e.g. 'Submitted batch job 12345')."""
    match = re.search(r"(\d+)\s*$", sbatch_stdout.strip())
    if not match:
        raise ValueError(f"Could not parse job ID from sbatch output: {sbatch_stdout!r}")
    return match.group(1)


def resolve_remote_experiment_dir(
    ssh: paramiko.SSHClient, remote_experiment_dir: str
) -> str:
    """Return the absolute path for a remote experiment directory (expands ~)."""
    _, stdout_stream, stderr_stream = ssh.exec_command(
        f"cd {remote_experiment_dir} && pwd"
    )
    exit_status = stdout_stream.channel.recv_exit_status()
    path = stdout_stream.read().decode("utf-8").strip()
    err = stderr_stream.read().decode("utf-8").strip()
    if exit_status != 0:
        raise RuntimeError(
            f"Could not resolve remote experiment dir {remote_experiment_dir!r}: {err}"
        )
    return path


def upload_experiment_scripts(
    ssh: paramiko.SSHClient, remote_experiment_dir: str
) -> None:
    """Copy local worker.py and job.slurm to the remote experiment directory."""
    sftp = ssh.open_sftp()
    try:
        try:
            sftp.stat(remote_experiment_dir)
        except FileNotFoundError:
            sftp.mkdir(remote_experiment_dir)

        remote_logs_dir = f"{remote_experiment_dir}/logs"
        try:
            sftp.stat(remote_logs_dir)
        except FileNotFoundError:
            sftp.mkdir(remote_logs_dir)

        for name in REMOTE_SCRIPT_NAMES:
            local_path = LOCAL_EXPERIMENT_DIR / name
            remote_path = f"{remote_experiment_dir}/{name}"
            sftp.put(str(local_path), remote_path)
            if name.endswith(".slurm"):
                sftp.chmod(remote_path, 0o755)
    finally:
        sftp.close()


def slurm_log_paths(remote_experiment_dir: str, job_id: str) -> dict[str, str]:
    """Return expected SLURM stdout/stderr file paths for a job."""
    log_dir = f"{remote_experiment_dir}/logs"
    return {
        "out": f"{log_dir}/slurm-{job_id}.out",
        "err": f"{log_dir}/slurm-{job_id}.err",
    }


FAILURE_STATES = frozenset(
    {"FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"}
)


def parse_sacct_job_state(sacct_stdout: str, job_id: str) -> tuple[bool, str, str]:
    """Return (succeeded, state, exit_code) for a single (non-array) SLURM job."""
    for line in sacct_stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue

        job_id_str, state, exit_code = parts[0], parts[1], parts[2]
        if job_id_str == job_id:
            succeeded = state not in FAILURE_STATES and exit_code.startswith("0:")
            return succeeded, state, exit_code

    raise ValueError(f"No sacct record found for job {job_id}")
