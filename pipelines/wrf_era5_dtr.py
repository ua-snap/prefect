"""Flow for processing WRF-downscaled ERA5 Diurnal Temperature Range from daily tasmax and tasmin data."""

from prefect import flow, task
import paramiko
from pathlib import Path
from utils import utils, cmip6


# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22

# name of folder in working_dir where dtr data is written
out_dir_name = "era5_dtr"


@task
def run_process_era5_dtr(
    ssh,
    launcher_script,
    worker_script,
    conda_env_name,
    era5_dir,
    output_dir,
    slurm_dir,
    partition,
):
    cmd = (
        f"conda activate {conda_env_name}; "
        f"python {launcher_script} "
        f"--worker_script {worker_script} "
        f"--conda_env_name {conda_env_name} "
        f"--era5_dir {era5_dir} "
        f"--output_dir {output_dir} "
        f"--slurm_dir {slurm_dir} "
        f"--partition {partition} "
    )

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
    if exit_status != 0:
        # this should error if something fails with creating the job
        raise Exception(f"Error in starting the DTR processing. Error: {stderr}")

    job_ids = utils.parse_job_ids(stdout)
    assert (
        len(job_ids) == 1
    ), f"More than one job ID given for batch file generation: {job_ids}"

    print(f"ERA5 processing job submitted! (job ID: {job_ids[0]})")

    return job_ids


@flow(log_prints=True)
def process_era5_dtr(
    ssh_username,
    ssh_private_key_path,
    repo_name,  # cmip6-utils
    branch_name,
    conda_env_name,
    era5_dir,
    scratch_dir,
    work_dir_name,
    partition,
):

    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        repo_path = utils.clone_github_repository(
            ssh, repo_name, branch_name, scratch_dir
        )

        utils.ensure_slurm(ssh)

        utils.ensure_conda(ssh)

        utils.ensure_conda_env(
            ssh, conda_env_name, repo_path.joinpath("environment.yml")
        )

        launcher_script = repo_path.joinpath("derived", "run_wrf_era5_dtr.py")
        worker_script = repo_path.joinpath("derived", "dtr.py")
        working_dir = Path(scratch_dir).joinpath(work_dir_name)
        output_dir = working_dir.joinpath(out_dir_name)
        slurm_dir = working_dir.joinpath("slurm")

        kwargs = {
            "ssh": ssh,
            "launcher_script": launcher_script,
            "worker_script": worker_script,
            "conda_env_name": conda_env_name,
            "era5_dir": era5_dir,
            "output_dir": output_dir,
            "slurm_dir": slurm_dir,
            "partition": partition,
        }
        job_ids = run_process_era5_dtr(**kwargs)

        utils.wait_for_jobs_completion(
            ssh,
            job_ids,
            completion_message="Slurm jobs for regridding complete.",
        )
    finally:
        ssh.close()

    return output_dir


if __name__ == "__main__":
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    conda_env_name = "cmip6-utils"
    era5_dir = "/beegfs/CMIP6/snapdata/daily_era5_4km_3338"
    scratch_dir = f"/beegfs/CMIP6/snapdata"
    work_dir_name = "cmip6_4km_downscaling"
    partition = "t2small"

    process_era5_dtr.serve(
        name="process-dtr-cmip6",
        tags=["Data production", "CMIP6"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "conda_env_name": conda_env_name,
            "era5_dir": era5_dir,
            "scratch_dir": scratch_dir,
            "work_dir_name": work_dir_name,
            "partition": partition,
        },
    )
