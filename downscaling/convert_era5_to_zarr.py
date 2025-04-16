"""Wrapper flow for converting ERA5 netCDFs to Zarr format.
Uses fixed year range and assumes certain directory structure.

"""

import logging
from prefect import flow, task
import paramiko
from pathlib import Path
from utils import utils

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22

tmp_year_str = "{var_id}_{year}_era5_4km_3338.nc"
tmp_zarr_name = "{var_id}_era5.zarr"
year_range = (1965, 2014)


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
        logging.info(stdout)

    job_ids = utils.parse_job_ids(stdout)
    assert (
        len(job_ids) == 1
    ), f"More than one job ID given for batch file generation: {job_ids}"

    logging.info(
        f"ERA5 Netcdf-to-Zarr conversion job submitted! (job ID: {job_ids[0]})"
    )

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
    tmp_dir_name,  # e.g. zarr_bias_adjust_inputs
    out_dir_name,
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

        # check that netcdf_dir is in scratch_dir
        # netcdf_dir_on_scratch = utils.input_is_child_of_scratch_dir(
        #     ssh, netcdf_dir, scratch_dir
        # )
        # if not netcdf_dir_on_scratch:
        #     # run an rsync task to get the data to scratch_dir
        #     utils.rsync(
        #         ssh,
        #         source_directory=netcdf_dir,
        #         destination_directory=scratch_dir.joinpath(netcdf_dir.name),
        #     )

        utils.check_for_nfs_mount(ssh, "/import/beegfs")

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
        tmp_dir = scratch_dir.joinpath(tmp_dir_name)
        output_dir = scratch_dir.joinpath(out_dir_name)
        slurm_dir = tmp_dir.joinpath("slurm")

        utils.create_directories(ssh, [tmp_dir, slurm_dir])

        kwargs = {
            "ssh": ssh,
            "launcher_script": launcher_script,
            "conda_env_name": conda_env_name,
            "worker_script": worker_script,
            "netcdf_dir": netcdf_dir,
            "variables": variables,
            "output_dir": output_dir,
            "partition": partition,
        }
        job_ids = [run_convert_era5_netcdf_to_zarr(**kwargs)]

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
    variables = "tasmax pr dtr"
    scratch_dir = "/import/beegfs/CMIP6/snapdata"
    tmp_dir_name = "cmip6_downscaling"
    out_dir_name = "zarr_bias_adjust_inputs"
    netcdf_dir = "/center1/CMIP6/snapdata/daily_era5_4km_3338"
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
            "tmp_dir_name": tmp_dir_name,
            "out_dir_name": out_dir_name,
            "variables": variables,
            "netcdf_dir": netcdf_dir,
        },
    )
