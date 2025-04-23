"""Flow for statistical downscaling CMIP6 data using the cmip6-utils repository.

Notes
- This flow is designed to be run on the Chinook cluster.
- It makes use of /center1/CMIP6 for more robust processing with Dask,
because we have had issues with Dask + IO on the /beegfs filesystem.

Thus, the following space is needed on /center1/CMIP6:
~ 40 GB for 50 years of daily 4km ERA5 data for 3 variables
~ 40 GB for converted Zarr store of that ERA5 data

which does not have the abundance of storage we are used to with / beegfs

Downscaled data is written to <scratch_dir>/<work_dir_name>/downscaled
"""

from prefect import flow, task
from prefect.logging import get_run_logger
import paramiko
from pathlib import Path
from utils import utils
from utils import cmip6
from regridding.regrid_cmip6_4km import regrid_cmip6_4km
from pipelines.cmip6_dtr import process_dtr
from pipelines.wrf_era5_dtr import process_era5_dtr
from downscaling.convert_cmip6_to_zarr import convert_cmip6_to_zarr
from downscaling.convert_era5_to_zarr import convert_era5_to_zarr
from bias_adjust.train_bias_adjustment import train_bias_adjustment
from bias_adjust.bias_adjustment import bias_adjustment

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22

# name of folder in working_dir where downscaled data is written
out_dir_name = "downscaled"


def get_regrid_variables(variables):
    """Modifies variables as needed that are unintended for regridding step using the string representation of variables list

    Parameters:
        variables (str): String representation of variables list

    Returns:
        str: String representation of variables list for regridding
    """
    var_list = cmip6.validate_vars(variables, return_list=True)
    drop_vars = ["dtr"]  # dtr is not a variable in the CMIP6 data
    regrid_variables_list = [var for var in var_list if var not in drop_vars]

    if "dtr" in var_list:
        if "tasmin" not in var_list:
            regrid_variables_list.append("tasmin")
        if "tasmax" not in var_list:
            regrid_variables_list.append("tasmax")

    regrid_variables = " ".join(regrid_variables_list)

    return regrid_variables


@task
def ensure_reference_data_in_scratch(
    ssh_username,
    ssh_private_key_path,
    reference_dir,  # e.g. /beegfs/CMIP6/kmredilla/daily_era5_4km_3338/netcdf
    scratch_dir,  # e.g. /center1/CMIP6/kmredilla
    working_dir,
):
    logger = get_run_logger()
    logger.info(
        f"Checking for reference data directory {reference_dir} in scratch_dir {scratch_dir}"
    )
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        ref_exists = utils.input_is_child_of_scratch_dir(
            ssh, reference_dir, scratch_dir
        )
        if not ref_exists:
            logger.info(
                f"Reference data not found in scratch_dir. Copying from {reference_dir}."
            )
            ref_scratch_dir = working_dir.joinpath("ref_netcdf")
            utils.rsync(ssh, reference_dir, ref_scratch_dir)
            logger.info(
                f"Copied reference data from {reference_dir} to {ref_scratch_dir}"
            )

        else:
            logger.info(
                "Reference data already exists in scratch_dir. No action needed."
            )
            ref_scratch_dir = reference_dir

    finally:
        # Close the SSH connection
        ssh.close()

    return ref_scratch_dir


@task
def link_dtr_to_regrid(
    ssh_username,
    ssh_private_key_path,
    dtr_dir,  # e.g. /center1/CMIP6/kmredilla/cmip6_4km_downscaling/dtr
    regrid_dir,  # e.g. /center1/CMIP6/kmredilla/cmip6_4km_downscaling/regrid
):
    logger = get_run_logger()
    logger.info(f"Linking DTR data from {dtr_dir} to {regrid_dir}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        cmd = (
            f"d1={regrid_dir}; "
            f"d2={dtr_dir}; "
            'find "$d2" -type f | while read -r filepath; do '
            'relpath="${filepath#$d2/}"; '
            'destdir="$(dirname "$d1/$relpath")"; '
            'mkdir -p "$destdir"; '
            'ln -sf "$filepath" "$d1/$relpath"; '
            "done"
        )

        utils.exec_command(ssh, cmd)

    finally:
        # Close the SSH connection
        ssh.close()


@task
def link_dir(
    ssh_username,
    ssh_private_key_path,
    src_dir,  # e.g. /center1/CMIP6/kmredilla/cmip6_4km_downscaling/era5_dtr
    target_dir,  # e.g. /center1/CMIP6/kmredilla/daily_era5_4km_3338/netcdf/dtr
):
    """Link a directory to another directory using SSH.

    Created to link new DTR data to the ERA5 directory for batch access."""
    logger = get_run_logger()
    logger.info(f"Linking directory from {src_dir} to {target_dir}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        cmd = f"ln -sf {target_dir} {target_dir}"

        utils.exec_command(ssh, cmd)

    finally:
        # Close the SSH connection
        ssh.close()


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
    work_dir_name,
    variables,
    models,
    scenarios,
    partition,
    target_grid_source_file,
):
    # logger = get_run_logger()

    reference_dir = Path(reference_dir)
    cmip6_dir = Path(cmip6_dir)
    scratch_dir = Path(scratch_dir)
    working_dir = scratch_dir.joinpath(work_dir_name)

    # to start, we should probably just get every step laid out here
    # TO-DO: add these checks in as able
    # check for reference data in zarr format on scratch space
    # if yes, continue
    # if no, check for reference data in netcdf in working_dir
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
        "work_dir_name": work_dir_name,
        "models": models,
        "scenarios": scenarios,
        "variables": variables,
        "partition": partition,
    }

    ### Regrid CMIP6 to 4km ERA5 grid
    regrid_cmip6_kwargs = base_kwargs.copy()

    # TO-DO: take target_grid_
    # target_grid_source_file = reference_dir.joinpath(
    #     "t2max/t2max_2014_era5_4km_3338.nc"
    # )
    regrid_variables = get_regrid_variables(variables)
    regrid_cmip6_kwargs.update(
        {
            "cmip6_dir": cmip6_dir,
            "target_grid_source_file": target_grid_source_file,
            "interp_method": "bilinear",
            "freqs": "day",
            "rasdafy": False,
            "variables": regrid_variables,
        }
    )
    # regrid_dir = regrid_cmip6_4km(**regrid_cmip6_kwargs)
    regrid_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/regrid"

    ### CMIP6 DTR processing
    process_dtr_kwargs = base_kwargs.copy()
    del process_dtr_kwargs["variables"]
    process_dtr_kwargs.update(
        {
            "input_dir": regrid_dir,
        }
    )
    # cmip6_dtr_dir = process_dtr(**process_dtr_kwargs)
    cmip6_dtr_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/dtr"

    # Note on directory structure:
    # to keep things organized separately for individual tasks/flows,
    # we will not be writing outputs to the same child directories,
    # but rather giving them their own directories and linking them
    # (the main case here being DTR, since that is created in this flow)

    # link the dtr data under the regrid directory so that it can all be accessed in the same place
    link_dtr_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "dtr_dir": cmip6_dtr_dir,
        "regrid_dir": regrid_dir,
    }
    # link_dtr_to_regrid(**link_dtr_kwargs)

    # ERA5 DTR processing
    process_era5_dtr_kwargs = base_kwargs.copy()
    del process_era5_dtr_kwargs["variables"]
    del process_era5_dtr_kwargs["models"]
    del process_era5_dtr_kwargs["scenarios"]
    process_era5_dtr_kwargs.update(
        {
            "era5_dir": reference_dir,
        }
    )
    # era5_dtr_dir = process_era5_dtr(**process_era5_dtr_kwargs)
    era5_dtr_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/era5_dtr"

    era5_target_dtr_dir = reference_dir.joinpath("dtr")
    link_era5_dtr_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "src_dir": era5_dtr_dir,
        "target_dir": era5_target_dtr_dir,
    }
    # link_dir(**link_era5_dtr_kwargs)

    ref_data_check_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "reference_dir": reference_dir,
        "scratch_dir": scratch_dir,
        "working_dir": working_dir,
    }
    reference_dir = ensure_reference_data_in_scratch(**ref_data_check_kwargs)

    # convert ERA5 data to zarr
    convert_era5_to_zarr_kwargs = base_kwargs.copy()
    era5_vars = cmip6.cmip6_to_era5_variables(variables)
    convert_era5_to_zarr_kwargs.update(
        variables=era5_vars,
        netcdf_dir=reference_dir,
    )
    del convert_era5_to_zarr_kwargs["models"]
    del convert_era5_to_zarr_kwargs["scenarios"]
    # ref_zarr_dir = convert_era5_to_zarr(**convert_era5_to_zarr_kwargs)
    ref_zarr_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/era5_zarr"

    # convert CMIP6 data to zarr
    convert_cmip6_to_zarr_kwargs = base_kwargs.copy()
    convert_cmip6_to_zarr_kwargs.update(
        netcdf_dir=regrid_dir,
    )
    cmip6_zarr_dir = convert_cmip6_to_zarr(**convert_cmip6_to_zarr_kwargs)
    # cmip6_zarr_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/cmip6_zarr"

    train_bias_adjust_kwargs = base_kwargs.copy()
    del train_bias_adjust_kwargs["scenarios"]
    train_bias_adjust_kwargs.update(
        {
            "sim_dir": cmip6_zarr_dir,
            "ref_dir": ref_zarr_dir,
        }
    )
    train_dir = train_bias_adjustment(**train_bias_adjust_kwargs)
    # train_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/trained_datasets"

    bias_adjust_kwargs = base_kwargs.copy()
    bias_adjust_kwargs.update(
        {
            "sim_dir": cmip6_zarr_dir,
            "train_dir": train_dir,
        }
    )
    bias_adjustment(**bias_adjust_kwargs)


if __name__ == "__main__":
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    conda_env_name = "cmip6-utils"
    cmip6_dir = "/beegfs/CMIP6/arctic-cmip6/CMIP6"
    reference_dir = "/beegfs/CMIP6/arctic-cmip6/era5/daily_era5_4km_3338"
    scratch_dir = "/center1/CMIP6/snapdata"
    work_dir_name = "cmip6_4km_downscaling"
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
        "work_dir_name": work_dir_name,
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
