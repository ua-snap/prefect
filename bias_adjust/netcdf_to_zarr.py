"""Contains tasks for converting netcdf files to zarr format. This is done for the bias adjustment to
improve performance and resilience of the pipeline.
"""

from prefect import flow, task
import paramiko
from pathlib import Path
from utils import utils
import logging

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@task
def run_netcdf_zarr_conversion(
    ssh,
    launcher_script,
    conda_env_name,
    netcdf_dir,  # e.g. /beegfs/CMIP6/kmredilla/daily_era5_4km_3338/
    zarr_path,
    slurm_dir,
    glob_str=None,  # e.g. t2max/t2max_*.nc
    year_str=None,  # e.g. t2max/t2max_{year}_era5_4km_3338.nc
    start_year=None,  # e.g. 1965
    end_year=None,  # e.g. 2014
    chunks_dict=None,
):
    script_dir = Path(launcher_script).parent
    cmd = (
        f"python {launcher_script} "
        f"--conda_env_name {conda_env_name} "
        f"--script_dir {script_dir} "
        f"--netcdf_dir {netcdf_dir} "
        f"--zarr_path {zarr_path} "
        f"--slurm_dir {slurm_dir} "
    )
    if glob_str:
        cmd += f"--glob_str {glob_str} "
    elif year_str:
        cmd += f"--year_str {year_str} "
        cmd += f"--start_year {start_year} --end_year {end_year} "
    if chunks_dict:
        cmd += f"--chunks_dict {chunks_dict} "

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
    if exit_status != 0:
        # this should error if something fails with creating the job
        raise Exception(
            f"Error in starting the Zarr conversion processing. Error: {stderr}"
        )
    if stdout != "":
        logging.info(stdout)

    job_ids = utils.parse_job_ids(stdout)
    assert (
        len(job_ids) == 1
    ), f"More than one job ID given for batch file generation: {job_ids}"

    print(f"Netcdf-to-Zarr conversion job submitted! (job ID: {job_ids[0]})")

    return job_ids


@flow(log_prints=True)
def netcdf_to_zarr(
    ssh_username,
    ssh_private_key_path,
    repo_name,
    branch_name,
    conda_env_name,
    netcdf_dir,
    scratch_directory,  # e.g. /import/beegfs/kmredilla
    write_dir_name,  # e.g. zarr_bias_adjust_inputs
    zarr_store_name,  # e.g. era5_t2max.zarr
    glob_str=None,
    year_str=None,
    start_year=None,
    end_year=None,
    chunks_dict=None,
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
            ssh, repo_name, branch_name, scratch_directory
        )

        utils.check_for_nfs_mount(ssh, "/import/beegfs")

        utils.ensure_slurm(ssh)

        utils.ensure_conda(ssh)

        utils.ensure_conda_env(
            ssh, conda_env_name, repo_path.joinpath("environment.yml")
        )

        launcher_script = repo_path.joinpath("bias_adjust", "run_netcdf_to_zarr.py")
        write_directory = Path(scratch_directory).joinpath(write_dir_name)
        output_directory = write_directory.joinpath("zarr")
        zarr_path = output_directory.joinpath(zarr_store_name)
        slurm_directory = write_directory.joinpath("slurm")

        utils.create_directories(
            ssh, [write_directory, output_directory, slurm_directory]
        )

        kwargs = {
            "ssh": ssh,
            "launcher_script": launcher_script,
            "conda_env_name": conda_env_name,
            "netcdf_dir": netcdf_dir,
            "glob_str": glob_str,
            "year_str": year_str,
            "start_year": start_year,
            "end_year": end_year,
            "chunks_dict": chunks_dict,
            "zarr_path": zarr_path,
            "slurm_dir": slurm_directory,
        }

        job_ids = run_netcdf_zarr_conversion(**kwargs)
        utils.wait_for_jobs_completion(
            ssh,
            job_ids,
            completion_message="Slurm jobs for Zarr conversion complete.",
        )

    finally:
        ssh.close()


if __name__ == "__main__":
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    conda_env_name = "cmip6-utils"
    scratch_directory = "/import/beegfs/CMIP6/snapdata"
    write_dir_name = "zarr_bias_adjust_inputs"

    netcdf_to_zarr.serve(
        name="convert-netcdf-to-zarr",
        tags=["Bias adjustment", "Downscaling"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "conda_env_name": conda_env_name,
            "scratch_directory": scratch_directory,
            "write_dir_name": write_dir_name,
        },
    )
