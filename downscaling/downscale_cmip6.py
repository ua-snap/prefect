"""Flow for statistical downscaling CMIP6 data using the cmip6-utils repository.

Notes
- This flow is designed to be run on the Chinook cluster.
- It makes use of /center1/CMIP6 for more robust processing with Dask,
because we have had issues with Dask + IO on the /beegfs filesystem.

Thus, the following space is needed on /center1/CMIP6:
~ 40 GB for 50 years of daily 4km ERA5 data for 3 variables
~ 40 GB for converted Zarr store of that ERA5 data

which does not have the abundance of storage we are used to with / beegfs

"""

from prefect import flow, task
import paramiko
from pathlib import Path
from utils import utils
import logging
from utils import cmip6
from regridding.regrid_cmip6_4km import regrid_cmip6_4km
from pipelines.cmip6_dtr import process_dtr
from downscaling.convert_cmip6_to_zarr import convert_cmip6_to_zarr
from downscaling.convert_era5_to_zarr import convert_era5_to_zarr

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@flow(log_prints=True)
def downscale_cmip6(
    ssh_username,
    ssh_private_key_path,
    repo_name,  # cmip6-utils
    branch_name,
    conda_env_name,
    cmip6_dir,  # e.g. /beegfs/CMIP6/arctic-cmip6/CMIP6
    reference_dir,  # e.g. /beegfs/CMIP6/kmredilla/daily_era5_4km_3338/netcdf
    scratch_dir,  # e.g. /center1/CMIP6/kmredilla
    out_dir_name,
    variables,
    models,
    scenarios,
    partition,
    target_grid_source_file,
):
    reference_dir = Path(reference_dir)
    cmip6_dir = Path(cmip6_dir)
    scratch_dir = Path(scratch_dir)

    # to start, we should probably just get every step laid out here
    # TO-DO: add these checks in as able
    # check for reference data in zarr format on scratch space
    # if yes, continue
    # if no, check for reference data in netcdf on scratch space
    # if yes, convert to zarr
    # if no, rsync from reference_dir

    # here are some base kwargs that will be recycled across subflows
    base_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "repo_name": repo_name,
        "branch_name": branch_name,
        "conda_env_name": conda_env_name,
        "scratch_dir": scratch_dir,
        "out_dir_name": out_dir_name,
        "models": models,
        "scenarios": scenarios,
        "variables": variables,
        "partition": partition,
    }

    # first subflow: regrid CMIP6 data to target grid
    regrid_cmip6_kwargs = base_kwargs.copy()

    # TO-DO: take target_grid_
    # target_grid_source_file = reference_dir.joinpath(
    #     "t2max/t2max_2014_era5_4km_3338.nc"
    # )
    regrid_cmip6_kwargs.update(
        {
            "cmip6_dir": cmip6_dir,
            "target_grid_source_file": target_grid_source_file,
            "interp_method": "bilinear",
            "freqs": "day",
            "rasdafy": False,
        }
    )
    regrid_dir = regrid_cmip6_4km(**regrid_cmip6_kwargs)

    # second subflow: process DTR
    process_dtr_kwargs = base_kwargs.copy()
    process_dtr_kwargs.update(
        {
            "input_dir": regrid_dir,
            "output_dir": regrid_dir,  # will write outputs back to input dir, as they mimic the input dir structure
            "scratch_dir": scratch_dir,
        }
    )
    process_dtr(**process_dtr_kwargs)

    {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "repo_name": repo_name,
        "branch_name": branch_name,
        "conda_env_name": conda_env_name,
        "models": models,
        "scenarios": scenarios,
        "input_dir": cmip6_dir,
        "scratch_dir": scratch_dir,
        "out_dir_name": out_dir_name,
        "partition": partition,
    }

    # subflow: copy CMIP6 data to scratch directory

    # subflow: convert cmip6 to zarr
    convert_cmip6_to_zarr_kwargs = (
        {
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "conda_env_name": conda_env_name,
            "netcdf_dir": cmip6_dir,
            "variables": variables,
            "models": models,
            "scenarios": scenarios,
            "scratch_dir": scratch_dir,
            "out_dir_name": out_dir_name,
            "partition": partition,
        },
    )
    # convert_cmip6_to_zarr(**convert_cmip6_to_zarr_kwargs)

    # subflow: run bias adjustment
    # bias_adjust_kwargs = base_kwargs.copy()


if __name__ == "__main__":
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    conda_env_name = "cmip6-utils"
    cmip6_dir = "/beegfs/CMIP6/arctic-cmip6/CMIP6"
    reference_dir = "/beegfs/CMIP6/arctic-cmip6/era5/daily_era5_4km_3338"
    scratch_dir = "/center1/CMIP6/snapdata"
    out_dir_name = "cmip6_4km_downscaling"
    variables = "tasmax dtr pr"
    models = "all"
    scenarios = "all"
    partition = "t2small"
    target_grid_source_file = "/beegfs/CMIP6/kmredilla/downscaling/era5_target_slice.nc"

    params_dict = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "repo_name": repo_name,
        "branch_name": branch_name,
        "conda_env_name": conda_env_name,
        "cmip6_dir": cmip6_dir,
        "reference_dir": reference_dir,
        "scratch_dir": scratch_dir,
        "out_dir_name": out_dir_name,
        "variables": variables,
        "models": models,
        "scenarios": scenarios,
        "partition": partition,
        "target_grid_source_file": target_grid_source_file,
    }
    downscale_cmip6.serve(
        name="downscale-cmip6",
        parameters=params_dict,
    )
