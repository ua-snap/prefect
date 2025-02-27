"""This is the script for regridding the CMIP6 data to a common ~100km grid."""

from pathlib import Path
import paramiko
from prefect import task, flow
from regrid_cmip6 import regrid_cmip6
from utils import utils

# production latitude slice for target grid
# hardcoded for panarctic extent
prod_lat_slice = slice(50, 90)
# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@task
def create_target_grid_file(
    ssh,
    conda_env_name,
    target_grid_source_file,
    prod_lat_slice,
    target_grid_file,
):
    """
    Task to create a target grid file for regridding.

    Parameters
    ----------
        ssh : Paramiko SSHClient object
            ssh connection to the remote processing server
        conda_env_name : str
            Name of the Conda environment to activate for processing
        target_grid_source_file : str
            Path to file on the target grid which will be cropped/sliced
        prod_lat_slice : slice
            Slice to use for cropping the target grid file
        target_grid_file : str
            Path to save the cropped target grid file
    """
    cmd = (
        f"conda activate {conda_env_name}; "
        f"python -c 'import xarray as xr; "
        f'ds = xr.open_dataset("{target_grid_source_file}"); '
        f'ds.sel(lat={prod_lat_slice}).isel(time=[0]).to_netcdf("{target_grid_file}")\''
    )

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    # Check the exit status for errors
    if exit_status != 0:
        raise Exception(
            f"Error submitting python code to create target grid file. Error: {stderr}"
        )

    return


@flow
def regrid_cmip6_common(
    # TO-DO: for all of these functions with tons of params we should really be using kwargs
    ssh_username,
    ssh_private_key_path,
    repo_name,  # cmip6-utils
    branch_name,
    cmip6_directory,
    target_grid_source_file,
    scratch_directory,
    out_dir_name,
    no_clobber,
    vars,
    interp_method,
    freqs,
    models,
    scenarios,
    conda_env_name,
    rasdafy,
    target_sftlf_fp=None,
):
    target_grid_file = f"{scratch_directory}/target_common_grid.nc"
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

        utils.ensure_conda(ssh)

        utils.ensure_conda_env(
            ssh, conda_env_name, repo_path.joinpath("environment.yml")
        )

        create_target_grid_file(
            ssh,
            conda_env_name,
            target_grid_source_file,
            prod_lat_slice,
            target_grid_file,
        )

    finally:
        ssh.close()

    kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "repo_name": repo_name,
        "branch_name": branch_name,
        "cmip6_directory": cmip6_directory,
        "scratch_directory": scratch_directory,
        "out_dir_name": out_dir_name,
        "target_grid_file": target_grid_file,
        "no_clobber": no_clobber,
        "vars": vars,
        "interp_method": interp_method,
        "freqs": freqs,
        "models": models,
        "scenarios": scenarios,
        "conda_env_name": conda_env_name,
        "rasdafy": rasdafy,
        "target_sftlf_fp": target_sftlf_fp,
    }

    regrid_cmip6(**kwargs)


if __name__ == "__main__":
    # prefect parameter inputs
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    cmip6_directory = Path("/beegfs/CMIP6/arctic-cmip6/CMIP6")
    scratch_directory = Path(f"/beegfs/CMIP6/snapdata/")
    out_dir_name = "cmip6_common_regrid"
    no_clobber = False
    vars = "all"
    interp_method = "bilinear"
    freqs = "all"
    models = "all"
    scenarios = "all"
    conda_env_name = "cmip6-utils"
    rasdafy = True
    target_grid_source_fp = f"{cmip6_directory}/ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/Amon/tas/gn/v20200528/tas_Amon_CESM2_ssp370_r11i1p1f1_gn_206501-210012.nc"
    target_sftlf_fp = f"{cmip6_directory}/CMIP/NCAR/CESM2/historical/r11i1p1f1/fx/sftlf/gn/v20190514/sftlf_fx_CESM2_historical_r11i1p1f1_gn.nc"

    regrid_cmip6_common.serve(
        name="regrid-cmip6-common",
        tags=["CMIP6 Regridding"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "cmip6_directory": cmip6_directory,
            "scratch_directory": scratch_directory,
            "target_grid_source_file": target_grid_source_fp,
            "out_dir_name": out_dir_name,
            "no_clobber": no_clobber,
            "vars": vars,
            "interp_method": interp_method,
            "freqs": freqs,
            "models": models,
            "scenarios": scenarios,
            "conda_env_name": conda_env_name,
            "rasdafy": rasdafy,
            "target_sftlf_fp": target_sftlf_fp,
        },
    )
