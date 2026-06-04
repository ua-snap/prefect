"""
Orchestrate Drought Indicator processing on Chinook.
"""

from pathlib import Path
from shlex import quote

import paramiko
from prefect import flow, get_run_logger, task

from utils import utils
from utils.utils import wait_for_single_slurm_job_completion

# SSH connection details for Chinook
SSH_HOST = "chinook04.rcs.alaska.edu"
SSH_PORT = 22


@task
def submit_sbatch(
    ssh: paramiko.SSHClient,
    repo_dir: str,
    sbatch_script: str,
    dependency_job_id: str | None = None,
) -> str:
    """Submit an sbatch script from the nws-drought repo and return the job ID."""
    logger = get_run_logger()

    # the pipeline run sbatch script dependends on the pipelie download via the slurm --dependency flag
    dependency_arg = ""
    if dependency_job_id is not None:
        if not dependency_job_id.isdigit():
            raise ValueError(
                f"Expected numeric dependency job ID, got: {dependency_job_id!r}"
            )
        dependency_arg = f"--dependency=afterok:{dependency_job_id}"

    cmd = f"""
    set -euo pipefail
    cd {quote(repo_dir)}
    cd nws-drought
    sbatch --parsable {dependency_arg} {quote(sbatch_script)}
    """

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    if exit_status != 0:
        raise RuntimeError(
            "Failed to submit SLURM job.\n"
            f"Script: {sbatch_script}\n"
            f"Exit status: {exit_status}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    job_id = stdout.strip().splitlines()[-1].split(";")[0]

    if not job_id.isdigit():
        raise RuntimeError(f"Expected numeric SLURM job ID, got: {stdout!r}")

    logger.info("Submitted %s as SLURM job %s", sbatch_script, job_id)
    return job_id


@flow(name="submit-drought-slurm-jobs")
def submit_drought_slurm_jobs(
    ssh_username: str,
    ssh_private_key_path: str,
    branch_name: str,
    repo_dir: str,
    ssh_host: str = SSH_HOST,
    ssh_port: int = SSH_PORT,
) -> dict[str, str]:
    """Submit the drought download job, then submit processing after download succeeds."""
    logger = get_run_logger()

    ssh = utils.connect_ssh(
        ssh_host=ssh_host,
        ssh_port=ssh_port,
        ssh_username=ssh_username,
        ssh_private_key_path=ssh_private_key_path,
    )
    utils.ensure_uv(ssh)
    utils.clone_github_repository(ssh, "nws-drought", branch_name, repo_dir)

    try:
        logger.info("Connected to %s as %s", ssh_host, ssh_username)

        download_job_id = submit_sbatch(
            ssh=ssh,
            repo_dir=repo_dir,
            sbatch_script="pipeline_download.sbatch",
        )
        wait_for_single_slurm_job_completion(
            ssh=ssh, job_id=download_job_id, poll_seconds=600
        )  # dowload time is highly variable, no need to poll super frequently, just chill

        run_job_id = submit_sbatch(
            ssh=ssh,
            repo_dir=repo_dir,
            sbatch_script="pipeline_run.sbatch",
            dependency_job_id=download_job_id,
        )
        wait_for_single_slurm_job_completion(
            ssh=ssh, job_id=run_job_id, poll_seconds=60
        )  # processing is pretty fast

        return {
            "download_job_id": download_job_id,
            "run_job_id": run_job_id,
        }

    finally:
        ssh.close()
        logger.info("Closed SSH connection to %s", ssh_host)


if __name__ == "__main__":
    submit_drought_slurm_jobs.serve(
        name="scheduled-drought-processing",
        cron="0 9 */2 * *",  # 9 AM UTC every other day
        parameters={
            "ssh_username": "snapdata",
            "ssh_private_key_path": str(Path.home() / ".ssh" / "id_rsa"),
            "branch_name": "main",
            "repo_dir": "/import/beegfs/CMIP6/snapdata/repos/",
        },
    )  # ty:ignore[unused-awaitable]
