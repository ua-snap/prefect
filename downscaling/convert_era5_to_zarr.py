"""Wrapper flow for converting ERA5 netCDFs to Zarr format.
Uses fixed year range and assumes certain directory structure.

"""

from prefect import flow, task
from prefect.logging import get_run_logger
import paramiko
from pathlib import Path
from utils import utils

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22

out_dir_name = "era5_zarr"


@task
def run_convert_era5_netcdf_to_zarr(
    ssh,
    conda_env_name,
    launcher_script,
    partition,
    worker_script,
    netcdf_dir,
    output_dir,
    variables,
    slurm_dir,
):
    """This function will ssh to the remote server and run the slurm launcher script"""
    logger = get_run_logger()

    cmd = (
        f"conda activate {conda_env_name}; "
        f"python {launcher_script} "
        f"--partition {partition} "
        f"--conda_env_name {conda_env_name} "
        f"--worker_script {worker_script} "
        f"--netcdf_dir {netcdf_dir} "
        f"--output_dir {output_dir} "
        f"--variables '{variables}' "
        f"--slurm_dir {slurm_dir}"
    )

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
    if exit_status != 0:
        # this should error if something fails with creating the job
        raise Exception(
            f"Error in starting the Zarr conversion processing. Error: {stderr}"
        )
    if stdout != "":
        logger.info(stdout)

    job_ids = utils.parse_job_ids(stdout)
    assert (
        len(job_ids) == 1
    ), f"More than one job ID given for batch file generation: {job_ids}"

    logger.info(f"ERA5 Netcdf-to-Zarr conversion job submitted! (job ID: {job_ids[0]})")

    return job_ids


@flow(log_prints=True)
def convert_era5_to_zarr(
    ssh_username,
    ssh_private_key_path,
    repo_name,
    branch_name,
    conda_env_name,
    netcdf_dir,
    variables,
    scratch_dir,  # e.g. /center1/CMIP6/kmredilla
    work_dir_name,  # e.g. zarr_bias_adjust_inputs
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

        launcher_script = repo_path.joinpath(
            "bias_adjust", "run_era5_netcdf_to_zarr.py"
        )
        worker_script = repo_path.joinpath("bias_adjust", "netcdf_to_zarr.py")
        scratch_dir = Path(scratch_dir)
        working_dir = scratch_dir.joinpath(work_dir_name)
        output_dir = working_dir.joinpath(out_dir_name)
        slurm_dir = working_dir.joinpath("slurm")

        utils.create_directories(ssh, [output_dir, slurm_dir])

        kwargs = {
            "ssh": ssh,
            "launcher_script": launcher_script,
            "conda_env_name": conda_env_name,
            "worker_script": worker_script,
            "netcdf_dir": netcdf_dir,
            "variables": variables,
            "output_dir": output_dir,
            "slurm_dir": slurm_dir,
            "partition": partition,
        }
        job_ids = run_convert_era5_netcdf_to_zarr(**kwargs)

        utils.wait_for_jobs_completion(
            ssh,
            job_ids,
            completion_message="Slurm jobs for Zarr conversion complete.",
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
    variables = "tasmax pr dtr"
    scratch_dir = "/import/beegfs/CMIP6/snapdata"
    work_dir_name = "cmip6_4km_downscaling"
    netcdf_dir = "/center1/CMIP6/snapdata/daily_era5_4km_3338/netcdf"
    partition = "t2small"

    convert_era5_to_zarr.serve(
        name="convert-era5-netcdf-to-zarr",
        tags=["Bias adjustment", "Downscaling"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "partition": partition,
            "conda_env_name": conda_env_name,
            "scratch_dir": scratch_dir,
            "work_dir_name": work_dir_name,
            "variables": variables,
            "netcdf_dir": netcdf_dir,
        },
    )
