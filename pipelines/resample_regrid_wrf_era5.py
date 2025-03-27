"""Flow for processing WRF ERA5 data. Resampling the hourly data to daily resolution and regridding (reprojecting) to EPSG:3338"""

from prefect import flow, task
import paramiko
from pathlib import Path
from utils import utils


# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@task
def run_resample_and_regrid(
    ssh,
    launcher_script,
    conda_env_name,
    wrf_era5_directory,
    output_directory,
    slurm_directory,
    script_directory,
    geo_file,
    start_year,
    end_year,
    no_clobber,
):
    cmd = (
        f"python {launcher_script} "
        f"--conda_env_name {conda_env_name} "
        f"--wrf_era5_directory {wrf_era5_directory} "
        f"--output_directory {output_directory} "
        f"--slurm_directory {slurm_directory} "
        f"--script_directory {script_directory} "
        f"--geo_file {geo_file} "
        f"--start_year {start_year} "
        f"--end_year {end_year}"
    )
    if no_clobber:
        cmd += " --no_clobber"

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
    if exit_status != 0:
        # this should error if something fails with creating the job
        raise Exception(f"Error in starting the ERA5 processing. Error: {stderr}")

    job_ids = utils.parse_job_ids(stdout)
    assert (
        len(job_ids) == 1
    ), f"More than one job ID given for batch file generation: {job_ids}"

    print(f"ERA5 processing job submitted! (job ID: {job_ids[0]})")

    return job_ids


@flow(log_prints=True)
def resample_regrid_wrf_era5(
    ssh_username,
    ssh_private_key_path,
    repo_name,  # cmip6-utils
    branch_name,
    conda_env_name,
    scratch_directory,  # e.g. /import/beegfs/kmredilla
    write_dir_name,  # e.g. daily_era5_4km_3338
    wrf_era5_directory,  # /beegfs/CMIP6/wrf_era5/04km
    geo_file,  # /beegfs/CMIP6/wrf_era5/geo_em.d02.nc
    start_year,
    end_year,
    no_clobber,
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

        launcher_script = repo_path.joinpath(
            "downscaling", "run_resample_and_regrid_era5.py"
        )
        write_directory = Path(scratch_directory).joinpath(write_dir_name)
        output_directory = write_directory.joinpath("netcdf")
        slurm_directory = write_directory.joinpath("slurm")
        script_directory = launcher_script.parent
        kwargs = {
            "ssh": ssh,
            "launcher_script": launcher_script,
            "conda_env_name": conda_env_name,
            "wrf_era5_directory": wrf_era5_directory,
            "output_directory": output_directory,
            "slurm_directory": slurm_directory,
            "script_directory": script_directory,
            "geo_file": geo_file,
            "start_year": start_year,
            "end_year": end_year,
            "no_clobber": no_clobber,
        }
        job_ids = run_resample_and_regrid(**kwargs)

        utils.wait_for_jobs_completion(
            ssh,
            job_ids,
            completion_message="Slurm jobs for regridding complete.",
        )
    finally:
        ssh.close()


if __name__ == "__main__":
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    conda_env_name = "cmip6-utils"
    wrf_era5_directory = Path("/beegfs/CMIP6/wrf_era5/04km")
    scratch_directory = Path(f"/beegfs/CMIP6/snapdata/")
    # output_directory = Path(f"/beegfs/CMIP6/snapdata/daily_era5_4km_3338")
    write_dir_name = "daily_era5_4km_3338"
    geo_file = Path("/beegfs/CMIP6/wrf_era5/geo_em.d02.nc")
    start_year = "1965"
    end_year = "2022"
    no_clobber = False

    resample_regrid_wrf_era5.serve(
        name="resample-regrid-wrf-era5",
        tags=["Data production", "ERA5"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "conda_env_name": conda_env_name,
            "scratch_directory": scratch_directory,
            "write_dir_name": write_dir_name,
            "wrf_era5_directory": wrf_era5_directory,
            "geo_file": geo_file,
            "start_year": start_year,
            "end_year": end_year,
            "no_clobber": no_clobber,
        },
    )
