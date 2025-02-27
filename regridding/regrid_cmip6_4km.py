"""This is the script for regridding the CMIP6 data to a 4km grid matching WRF ERA5 data.
Hard-coded defaults. 
"""

# temp target file: /beegfs/CMIP6/kmredilla/downscaling/era5_target_slice.nc

from pathlib import Path
import paramiko
from prefect import task, flow
from regrid_cmip6 import regrid_cmip6
from utils import utils

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@flow
def regrid_cmip6_4km(
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
    # target_grid_file = f"{scratch_directory}/target_common_grid.nc"
    # TO-DO: when it's ready, remove line below and uyse the one above
    target_grid_file = target_grid_source_file

    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

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
    out_dir_name = "cmip6_4km_3338"
    no_clobber = False
    vars = "tasmin tasmax pr"
    interp_method = "bilinear"
    freqs = "day"
    models = "all"
    scenarios = "all"
    conda_env_name = "snap-geo"
    rasdafy = False
    target_grid_source_fp = "/beegfs/CMIP6/kmredilla/downscaling/era5_target_slice.nc"
    target_sftlf_fp = None

    regrid_cmip6_4km.serve(
        name="regrid-cmip6-4km-era5",
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
