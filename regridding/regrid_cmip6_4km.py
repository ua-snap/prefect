"""This is the script for regridding the CMIP6 data to a 4km grid matching WRF ERA5 data.
Hard-coded defaults.
"""

# temp target file: /beegfs/CMIP6/kmredilla/downscaling/era5_target_slice.nc

from pathlib import Path
import paramiko
from prefect import task, flow
from regridding.regrid_cmip6 import regrid_cmip6
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
    cmip6_dir,
    target_grid_source_file,
    scratch_dir,
    out_dir_name,
    variables,
    interp_method,
    freqs,
    models,
    scenarios,
    conda_env_name,
    rasdafy,
    partition="t2small",
    target_sftlf_fp=None,
    no_clobber=False,
):
    # target_grid_file = f"{scratch_dir}/target_common_grid.nc"
    # TO-DO: when it's ready, remove line below and use the one above
    target_grid_file = target_grid_source_file

    kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "repo_name": repo_name,
        "branch_name": branch_name,
        "cmip6_dir": cmip6_dir,
        "scratch_dir": scratch_dir,
        "out_dir_name": out_dir_name,
        "target_grid_file": target_grid_file,
        "no_clobber": no_clobber,
        "variables": variables,
        "interp_method": interp_method,
        "freqs": freqs,
        "models": models,
        "scenarios": scenarios,
        "conda_env_name": conda_env_name,
        "rasdafy": rasdafy,
        "target_sftlf_fp": target_sftlf_fp,
        "partition": partition,
    }

    regrid_dir = regrid_cmip6(**kwargs)

    return regrid_dir


if __name__ == "__main__":
    # prefect parameter inputs
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    cmip6_dir = "/beegfs/CMIP6/arctic-cmip6/CMIP6"
    scratch_dir = f"/beegfs/CMIP6/snapdata/"
    out_dir_name = "cmip6_4km_3338"
    no_clobber = False
    variables = "tasmin tasmax pr"
    interp_method = "bilinear"
    freqs = "day"
    models = "all"
    scenarios = "all"
    conda_env_name = "snap-geo"
    rasdafy = False
    target_grid_source_fp = "/beegfs/CMIP6/kmredilla/downscaling/era5_target_slice.nc"
    target_sftlf_fp = None
    partition = "t2small"

    regrid_cmip6_4km.serve(
        name="regrid-cmip6-4km-era5",
        tags=["CMIP6 Regridding"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "cmip6_dir": cmip6_dir,
            "scratch_dir": scratch_dir,
            "target_grid_source_file": target_grid_source_fp,
            "out_dir_name": out_dir_name,
            "no_clobber": no_clobber,
            "variables": variables,
            "interp_method": interp_method,
            "freqs": freqs,
            "models": models,
            "scenarios": scenarios,
            "conda_env_name": conda_env_name,
            "rasdafy": rasdafy,
            "target_sftlf_fp": target_sftlf_fp,
            "partition": partition,
        },
    )
