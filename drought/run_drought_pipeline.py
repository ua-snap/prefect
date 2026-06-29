"""
Orchestrate Drought Indicator processing on Chinook.
"""

from pathlib import Path
from shlex import quote

import paramiko
from prefect import flow, get_run_logger, task

from utils import utils

SSH_HOST = "chinook04.rcs.alaska.edu"
SSH_PORT = 22
# CP note: I'm OK hardcoding these paths, but "CKAN" could change
DROUGHT_CKAN_DIR = "/import/SNAP/poseidon/CKAN/CKAN_Data/Base/Other/drought"
BASELINE_ZIP_PATH = f"{DROUGHT_CKAN_DIR}/drought_indicators_baseline_data.zip"


@task
def submit_sbatch(
    ssh: paramiko.SSHClient,
    repos_parent_dir: str,
    sbatch_script: str,
    dependency_job_id: str | None = None,
) -> str:
    """Submit an sbatch script from the nws-drought repo and return the job ID."""
    logger = get_run_logger()

    # the pipeline run sbatch script depends on the pipeline download via the slurm --dependency flag
    dependency_arg = ""
    if dependency_job_id is not None:
        if not dependency_job_id.isdigit():
            raise ValueError(
                f"Expected numeric dependency job ID, got: {dependency_job_id!r}"
            )
        dependency_arg = f"--dependency=afterok:{dependency_job_id}"

    cmd = f"""
    set -euo pipefail
    cd {quote(repos_parent_dir)}
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


@task
def submit_plot_sbatch(
    ssh: paramiko.SSHClient,
    repos_parent_dir: str,
    dependency_job_id: str,
) -> str:
    """Submit a wrapped sbatch job to run data_viz/plot_all.py and return the job ID."""
    logger = get_run_logger()

    if not dependency_job_id.isdigit():
        raise ValueError(
            f"Expected numeric dependency job ID, got: {dependency_job_id!r}"
        )

    wrap_cmd = "mkdir -p logs && uv run --frozen python data_viz/plot_all.py"

    cmd = f"""
    set -euo pipefail
    cd {quote(repos_parent_dir)}
    cd nws-drought
    sbatch --parsable \\
      --dependency=afterok:{dependency_job_id} \\
      --job-name=drought_plot \\
      --nodes=1 \\
      --ntasks=1 \\
      --partition=t2small \\
      --time=01:00:00 \\
      --cpus-per-task=1 \\
      --mem=64G \\
      --output=logs/%x-%j.out \\
      --error=logs/%x-%j.err \\
      --wrap={quote(wrap_cmd)}
    """

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    if exit_status != 0:
        raise RuntimeError(
            "Failed to submit SLURM plot job.\n"
            f"Exit status: {exit_status}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    job_id = stdout.strip().splitlines()[-1].split(";")[0]

    if not job_id.isdigit():
        raise RuntimeError(f"Expected numeric SLURM job ID, got: {stdout!r}")

    logger.info("Submitted plot job as SLURM job %s", job_id)
    return job_id


@task
def ensure_recent_data_empty(ssh: paramiko.SSHClient, repos_parent_dir: str) -> None:
    """Remove existing contents of recent_data before starting a fresh download."""
    logger = get_run_logger()
    recent_data_dir = f"{repos_parent_dir.rstrip('/')}/nws-drought/recent_data"

    cmd = f"""
    set -euo pipefail
    rm -rf {quote(recent_data_dir)}
    mkdir -p {quote(recent_data_dir)}
    """

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    if exit_status != 0:
        raise RuntimeError(
            "Failed to clear recent_data directory.\n"
            f"Directory: {recent_data_dir}\n"
            f"Exit status: {exit_status}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    logger.info("Cleared recent_data at %s", recent_data_dir)


@task
def ensure_baseline_data_extracted(
    ssh: paramiko.SSHClient,
    repos_parent_dir: str,
    baseline_zip_path: str = BASELINE_ZIP_PATH,
) -> None:
    """Ensure drought baseline data zip is extracted to baseline_data in the nws-drought repo."""
    logger = get_run_logger()
    baseline_data_dir = f"{repos_parent_dir.rstrip('/')}/nws-drought/baseline_data"

    cmd = f"""
    set -euo pipefail
    baseline_dir={quote(baseline_data_dir)}
    baseline_zip={quote(baseline_zip_path)}

    if [ ! -f "$baseline_zip" ]; then
      echo "Baseline zip not found: $baseline_zip" >&2
      exit 1
    fi

    if [ -d "$baseline_dir" ] && [ -n "$(find "$baseline_dir" -mindepth 1 -print -quit 2>/dev/null)" ]; then
      echo "Baseline data already extracted at $baseline_dir"
      exit 0
    fi

    mkdir -p "$baseline_dir"
    unzip -q "$baseline_zip" -d "$baseline_dir"
    """

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    if exit_status != 0:
        raise RuntimeError(
            "Failed to extract baseline data.\n"
            f"Zip: {baseline_zip_path}\n"
            f"Directory: {baseline_data_dir}\n"
            f"Exit status: {exit_status}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    if stdout.strip():
        logger.info(stdout.strip())
    logger.info("Baseline data available at %s", baseline_data_dir)


@task
def copy_figures_to_results(
    ssh: paramiko.SSHClient,
    repos_parent_dir: str,
    results_base: str = DROUGHT_CKAN_DIR,
) -> str:
    """Copy data_viz/figures to results_<analysis_date> under the CKAN drought dir.

    The analysis date is parsed from the .nc filename in nws-drought/recent_data
    (e.g. combined_daily_era5_land_drought_vars_20260516.nc -> 20260516).
    Returns the destination directory.
    """
    logger = get_run_logger()
    repo_dir = f"{repos_parent_dir.rstrip('/')}/nws-drought"

    cmd = f"""
    set -euo pipefail
    cd {quote(repo_dir)}

    nc_file=$(ls recent_data/*.nc 2>/dev/null | head -n1)
    if [ -z "$nc_file" ]; then
      echo "No .nc file found in recent_data" >&2
      exit 1
    fi

    analysis_date=$(basename "$nc_file" .nc | grep -oE '[0-9]{{8}}$')
    if [ -z "$analysis_date" ]; then
      echo "Could not parse 8-digit date from $nc_file" >&2
      exit 1
    fi

    if [ ! -d data_viz/figures ]; then
      echo "Figures directory data_viz/figures not found" >&2
      exit 1
    fi

    dest={quote(results_base.rstrip("/"))}/results_${{analysis_date}}
    mkdir -p -m 777 "$dest"
    cp -r data_viz/figures/. "$dest"/

    echo "$dest"
    """

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    if exit_status != 0:
        raise RuntimeError(
            "Failed to copy figures to results directory.\n"
            f"Exit status: {exit_status}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    dest = stdout.strip().splitlines()[-1]
    logger.info("Copied figures to %s", dest)
    return dest


@flow(name="submit-drought-slurm-jobs")
def submit_drought_slurm_jobs(
    ssh_username: str,
    ssh_private_key_path: str,
    branch_name: str,
    repos_parent_dir: str,
    ssh_host: str = SSH_HOST,
    ssh_port: int = SSH_PORT,
) -> dict[str, str]:
    """Submit download, processing, and plotting SLURM jobs with afterok dependencies.

    Returns job IDs immediately without waiting for SLURM completion.
    """
    logger = get_run_logger()

    ssh = utils.connect_ssh(
        ssh_host=ssh_host,
        ssh_port=ssh_port,
        ssh_username=ssh_username,
        ssh_private_key_path=ssh_private_key_path,
    )
    utils.ensure_uv(ssh)
    utils.clone_github_repository(ssh, "nws-drought", branch_name, repos_parent_dir)

    try:
        logger.info("Connected to %s as %s", ssh_host, ssh_username)

        ensure_baseline_data_extracted(ssh=ssh, repos_parent_dir=repos_parent_dir)
        ensure_recent_data_empty(ssh=ssh, repos_parent_dir=repos_parent_dir)

        download_job_id = submit_sbatch(
            ssh=ssh,
            repos_parent_dir=repos_parent_dir,
            sbatch_script="pipeline_download.sbatch",
        )

        run_job_id = submit_sbatch(
            ssh=ssh,
            repos_parent_dir=repos_parent_dir,
            sbatch_script="pipeline_run.sbatch",
            dependency_job_id=download_job_id,
        )

        plot_job_id = submit_plot_sbatch(
            ssh=ssh,
            repos_parent_dir=repos_parent_dir,
            dependency_job_id=run_job_id,
        )

        # Block until plotting finishes so data_viz/figures actually exists.
        # wait_for_jobs_completion mutates the list it receives, so pass a copy.
        utils.wait_for_jobs_completion(ssh, [plot_job_id])

        results_dir = copy_figures_to_results(
            ssh=ssh,
            repos_parent_dir=repos_parent_dir,
        )

        return {
            "download_job_id": download_job_id,
            "run_job_id": run_job_id,
            "plot_job_id": plot_job_id,
            "results_dir": results_dir,
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
            "repos_parent_dir": "/import/beegfs/CMIP6/snapdata/",
        },
    )  # ty:ignore[unused-awaitable] # CP note: this comment for my `ty` typechecker
