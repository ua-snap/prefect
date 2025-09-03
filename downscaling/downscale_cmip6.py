"""Flow for statistical downscaling CMIP6 data using the cmip6-utils repository.

Notes
- This flow is designed to be run on the Chinook cluster.
- It makes use of /center1/CMIP6 for more robust processing with Dask,
because we have had issues with Dask + IO on the /beegfs filesystem.
It does not have the abundance of storage we are used to with /beegfs

I don't have an exact number yet, but you will need to ensure there is enough
space in /center1/CMIP6 before running this flow for a single model. I think
a safe number is 1TB for all four variables (tasmax, dtr, pr, tasmin) and all possible scenarios.

The flow consists of the following steps:
1. Create the intermediate target grid file for the first regridding step.
2. Regrid CMIP6 data to the intermediate grid.
3. Regrid CMIP6 data to the final grid from the intermediate grid.
4. Process DTR from the regridded CMIP6 data.
5. Ensure ERA5 reference data is in scratch space (copy if not).
6. Process DTR from the ERA5 data (this could be removed if DTR processing is brought to ERA5 preprocessing repo)
7. Convert ERA5 data to Zarr format.
8. Convert CMIP6 data to Zarr format.
9. Train bias adjustment model using historical data only. Weights/ adjustment factors are saved on a model + variable basis.
10. Apply bias adjustment to the regridded CMIP6 data using the trained model.
11. Derive tasmin from the adjusted CMIP6 data by subtracting DTR from tasmax (if DTR is requested in flow parameters)
"""

import time
from prefect import flow, task
from prefect.logging import get_run_logger
import paramiko
from pathlib import Path
from utils import utils
from utils import cmip6
from regridding.regrid_cmip6 import regrid_cmip6
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


@flow
def clone_and_install_repo(
    ssh_username,
    ssh_private_key_path,
    repo_name,
    conda_env_name,
    branch_name,
    destination_directory,
):
    logger = get_run_logger()
    logger.info(f"Checking that {repo_name} repo has been cloned")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        logger.info(f"Checking that {repo_name} repo has been cloned")
        utils.clone_github_repository(
            ssh, repo_name, branch_name, destination_directory
        )

        repo_path = destination_directory.joinpath(repo_name)

        logger.info(f"Ensuring conda and conda environment {conda_env_name} are set up")
        utils.ensure_conda(ssh)
        utils.ensure_conda_env(
            ssh, conda_env_name, repo_path.joinpath("environment.yml")
        )
    finally:
        # Close the SSH connection
        ssh.close()


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
            utils.rsync(ssh, f"{reference_dir}/", str(ref_scratch_dir))
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


@task
def create_cascade_target_grid_file(
    ssh_username,
    ssh_private_key_path,
    cascade_grid_script,
    cascade_grid_source_file,
    scratch_dir,
    work_dir_name,
):
    cascade_target_file = scratch_dir.joinpath(work_dir_name, "intermediate_target.nc")

    logger = get_run_logger()
    logger.info(f"Creating target grid file {cascade_target_file}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # /center1/CMIP6/kmredilla/cmip6_4km_downscaling/intermediate_target.nc
    cmd = f"conda activate cmip6-utils && \
            python {cascade_grid_script} \
            --src_file {cascade_grid_source_file} \
            --out_file {cascade_target_file}"
    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)
        exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
        if exit_status != 0:
            raise Exception(
                f"Error in creating intermediate grid file for cascade regridding. Error: {stderr}"
            )
        if stdout != "":
            logger.info(stdout)

    finally:
        # Close the SSH connection
        ssh.close()

    return cascade_target_file


@flow
# putting this flow here now because it has more to do with downscaling than general regridding
def run_regrid_cmip6_again(
    working_dir,
    launcher_script,
    conda_env_name,
    partition,
    regrid_script,
    interp_method,
    target_grid_file,
    regridded_dir,
    ssh_username,
    ssh_private_key_path,
    out_dir_name,
):
    """Flow for regridding CMIP6 data that has been regridded once and so is all on a common grid."""
    logger = get_run_logger()
    logger.info(f"Regridding CMIP6 data again to {target_grid_file}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    slurm_dir = working_dir.joinpath("slurm")
    regrid_again_batch_dir = slurm_dir.joinpath("regrid_again_batch")
    output_dir = working_dir.joinpath(out_dir_name)

    cmd = (
        f"conda activate {conda_env_name}; "
        f"python {launcher_script} "
        f"--partition {partition} "
        f"--conda_env_name {conda_env_name} "
        f"--slurm_dir {slurm_dir} "
        f"--regrid_script {regrid_script} "
        f"--interp_method {interp_method} "
        f"--target_grid_file {target_grid_file} "
        f"--regridded_dir {regridded_dir} "
        f"--regrid_again_batch_dir {regrid_again_batch_dir} "
        f"--output_dir {output_dir} "
    )

    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)
        exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
        if exit_status != 0:
            raise Exception(f"Error in starting the re-regridding. Error: {stderr}")
        if stdout != "":
            logger.info(stdout)

        job_ids = utils.parse_job_ids(stdout)
        assert (
            len(job_ids) == 1
        ), f"More than one job ID given for final regridding: {job_ids}"

        logger.info(
            f"CMIP6 regridding to final grid job submitted! (job ID: {job_ids[0]})"
        )

        utils.wait_for_jobs_completion(
            ssh,
            job_ids,
            completion_message="Slurm jobs for final regridding complete.",
        )

    finally:
        # Close the SSH connection
        ssh.close()

    return output_dir


@task
def create_remote_directories(ssh_username, ssh_private_key_path, directories):
    """Create directories on the remote server. This will be the working directory and slurm directory."""
    logger = get_run_logger()
    logger.info(f"Creating the following directories: {directories}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        utils.create_directories(ssh, directories)

    finally:
        ssh.close()


@flow
def derive_cmip6_tasmin(
    ssh_username,
    ssh_private_key_path,
    input_dir,
    output_dir,
    slurm_dir,
    scratch_dir,
    repo_name,
    models,
    scenarios,
    conda_env_name,
    partition,
):
    """Derive tasmin from CMIP6 data."""
    logger = get_run_logger()
    logger.info(f"Deriving tasmin from CMIP6 data in {input_dir}")

    models = cmip6.validate_models(models, return_list=False)
    scenarios = cmip6.validate_scenarios(scenarios, return_list=False)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        repo_dir = scratch_dir.joinpath(repo_name)
        launcher_script = repo_dir.joinpath("derived", "run_cmip6_difference.py")
        worker_script = repo_dir.joinpath("derived", "difference.py")
        cmd = (
            f"python {launcher_script} "
            f"--worker_script {worker_script} "
            f"--conda_env_name {conda_env_name} "
            f"--input_dir {input_dir} "
            f"--output_dir {output_dir} "
            f"--slurm_dir {slurm_dir} "
            "--minuend_tmp_fn tasmax_\${model}_\${scenario}_adjusted.zarr "
            "--subtrahend_tmp_fn dtr_\${model}_\${scenario}_adjusted.zarr "
            "--out_tmp_fn tasmin_\${model}_\${scenario}_adjusted.zarr "
            "--new_var_id tasmin "
            f"--models '{models}' "
            f"--scenarios '{scenarios}' "
            f"--partition {partition}"
        )

        exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
        if exit_status != 0:
            raise Exception(
                f"Error in creating slurm job for deriving tasmin. Error: {stderr}"
            )
        if stdout != "":
            logger.info(stdout)

        job_ids = utils.parse_job_ids(stdout)

        utils.wait_for_jobs_completion(ssh, job_ids)

    finally:
        # Close the SSH connection
        ssh.close()


@flow
def derive_era5_tasmin(
    ssh_username,
    ssh_private_key_path,
    era5_zarr_dir,
    slurm_dir,
    repo_dir,
    conda_env_name,
    partition,
):
    # I honestly don't remember why I made a function for this. ERA5 tasmin (t2min) exists separately
    # and does not need to be derived again, could just be converted to zarr.
    """Derive tasmin from ERA5 data."""
    logger = get_run_logger()
    logger.info(f"Deriving tasmin from ERA5 data in {era5_zarr_dir}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        launcher_script = repo_dir.joinpath("derived", "run_wrf_era5_difference.py")
        worker_script = repo_dir.joinpath("derived", "difference.py")
        cmd = (
            f"python {launcher_script} "
            f"--worker_script {worker_script} "
            f"--conda_env_name {conda_env_name} "
            f"--slurm_dir {slurm_dir} "
            f"--minuend_store {era5_zarr_dir.joinpath('t2max_era5.zarr')} "
            f"--subtrahend_store {era5_zarr_dir.joinpath('dtr_era5.zarr')} "
            f"--output_store {era5_zarr_dir.joinpath('tasmin_era5.zarr')} "
            "--new_var_id tasmin "
            f"--partition {partition}"
        )

        exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
        if exit_status != 0:
            raise Exception(
                f"Error in creating slurm job for deriving ERA5 tasmin. Error: {stderr}"
            )
        if stdout != "":
            logger.info(stdout)

        job_ids = utils.parse_job_ids(stdout)

        utils.wait_for_jobs_completion(ssh, job_ids)

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
    flow_steps,
):
    # logger = get_run_logger()

    reference_dir = Path(reference_dir)
    cmip6_dir = Path(cmip6_dir)
    scratch_dir = Path(scratch_dir)
    working_dir = scratch_dir.joinpath(work_dir_name)
    slurm_dir = working_dir.joinpath("slurm")
    flow_steps_list = flow_steps.split()

    # this creates the maing working directory
    directories = [working_dir, slurm_dir]

    if flow_steps == "all" or "create_remote_directories" in flow_steps_list:
        create_remote_directories(
            ssh_username, ssh_private_key_path, directories=directories
        )

    clone_and_install_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "repo_name": repo_name,
        "branch_name": branch_name,
        "destination_directory": scratch_dir,
        "conda_env_name": conda_env_name,
    }

    if flow_steps == "all" or "clone_and_install_repo" in flow_steps_list:
        clone_and_install_repo(**clone_and_install_kwargs)

    # to start, we should probably just get every step laid out here
    # TO-DO: add these checks in as able
    # check for reference data in zarr format on scratch space
    # if yes, continue
    # if no, check for reference data in netcdf in working_dir
    # if yes, convert to zarr
    # if no, rsync from reference_dir

    # here are some base kwargs that will be recycled across subflows

    base_kwargs = {
        # "ssh_host": ssh_host,
        # "ssh_port": ssh_port,
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

    ### Regridding 1: Regrid CMIP6 data to intermediate grid
    # first, run the task to create the intermediate target grid file
    cascade_grid_script = scratch_dir.joinpath(
        repo_name, "downscaling", "make_intermediate_target_grid_file.py"
    )
    cascade_grid_source_file = cmip6_dir.joinpath(
        "ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/Amon/tas/gn/v20200528/tas_Amon_CESM2_ssp370_r11i1p1f1_gn_206501-210012.nc"
    )

    cascade_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "cascade_grid_script": cascade_grid_script,
        "cascade_grid_source_file": cascade_grid_source_file,
        "scratch_dir": scratch_dir,
        "work_dir_name": work_dir_name,
    }

    if flow_steps == "all" or "create_cascade_target_grid_file" in flow_steps_list:
        cascade_target_file = create_cascade_target_grid_file(**cascade_kwargs)
    else:
        cascade_target_file = f"{scratch_dir}/{work_dir_name}/intermediate_target.nc"

    intermediate_out_dir_name = "intermediate_regrid"
    regrid_cmip6_intermediate_kwargs = base_kwargs.copy()
    regrid_variables = get_regrid_variables(variables)
    interp_method = "bilinear"
    regrid_cmip6_intermediate_kwargs.update(
        {
            "cmip6_dir": cmip6_dir,
            "target_grid_file": cascade_target_file,
            "interp_method": interp_method,
            "out_dir_name": intermediate_out_dir_name,
            "freqs": "day",
            "rasdafy": False,
            "no_clobber": False,
            "variables": regrid_variables,
        }
    )

    if flow_steps == "all" or "regrid_cmip6" in flow_steps_list:
        intermediate_regrid_dir = regrid_cmip6(**regrid_cmip6_intermediate_kwargs)
    else:
        intermediate_regrid_dir = f"{scratch_dir}/{work_dir_name}/intermediate_regrid"

    ### Regridding 2: Regrid CMIP6 data to final grid

    # TO-DO: take target grid file from the reference data, e.g.:
    # target_grid_source_file = reference_dir.joinpath(
    #     "t2max/t2max_2014_era5_4km_3338.nc"
    # )

    regrid_again_script = scratch_dir.joinpath(
        repo_name, "regridding", "run_regrid_again.py"
    )
    regrid_script = scratch_dir.joinpath(repo_name, "regridding", "regrid.py")

    regrid_again_out_dir_name = "final_regrid"
    target_grid_file = target_grid_source_file
    regrid_again_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "conda_env_name": conda_env_name,
        "partition": partition,
        "launcher_script": regrid_again_script,
        "regrid_script": regrid_script,
        "interp_method": interp_method,
        "target_grid_file": target_grid_file,
        "working_dir": working_dir,
        "regridded_dir": intermediate_regrid_dir,
        "out_dir_name": regrid_again_out_dir_name,
    }

    if flow_steps == "all" or "run_regrid_cmip6_again" in flow_steps_list:
        final_regrid_dir = run_regrid_cmip6_again(**regrid_again_kwargs)
    else:
        final_regrid_dir = f"{scratch_dir}/{work_dir_name}/final_regrid"

    # final_target_file

    # regrid_cmip6_4km_kwargs = base_kwargs.copy()
    # regrid_cmip6_4km_kwargs.update(
    #     {
    #         "cmip6_dir": cmip6_dir,
    #         "target_grid_source_file": target_grid_source_file,
    #         "interp_method": "bilinear",
    #         "out_dir_name": "regrid",
    #         "freqs": "day",
    #         "rasdafy": False,
    #         "variables": regrid_variables,
    #     }
    # )
    # check for regridded data
    # missing_regrid_data = cmip6.check_for_derived_cmip6_data(**regrid_cmip6_kwargs)
    # if missing_regrid_data:
    #     regrid_dir = regrid_cmip6(**regrid_cmip6_kwargs)
    # regrid_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/regrid"

    # TO-DO: take target_grid_
    # target_grid_source_file = reference_dir.joinpath(
    #     "t2max/t2max_2014_era5_4km_3338.nc"
    # )
    ### Regrid CMIP6 to 4km ERA5 grid
    # regrid_cmip6_kwargs = base_kwargs.copy()
    # regrid_variables = get_regrid_variables(variables)
    # regrid_cmip6_kwargs.update(
    #     {
    #         "cmip6_dir": cmip6_dir,
    #         "target_grid_source_file": target_grid_source_file,
    #         "interp_method": "bilinear",
    #         "out_dir_name": "regrid",
    #         "freqs": "day",
    #         "rasdafy": False,
    #         "variables": regrid_variables,
    #     }
    # )
    # check for regridded data
    # missing_regrid_data = cmip6.check_for_derived_cmip6_data(**regrid_cmip6_kwargs)
    # if missing_regrid_data:
    #     regrid_dir = regrid_cmip6_4km(**regrid_cmip6_kwargs)
    # regrid_dir = "/center1/CMIP6/kmredilla/cmip6_4km_downscaling/regrid"

    ### CMIP6 DTR processing
    # This is done after we have regridded the data to the final grid
    process_dtr_kwargs = base_kwargs.copy()
    del process_dtr_kwargs["variables"]
    process_dtr_kwargs.update(
        {
            "input_dir": final_regrid_dir,
        }
    )

    if flow_steps == "all" or "process_dtr" in flow_steps_list:
        cmip6_dtr_dir = process_dtr(**process_dtr_kwargs)
    else:
        cmip6_dtr_dir = f"{scratch_dir}/{work_dir_name}/cmip6_dtr"

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
        "regrid_dir": final_regrid_dir,
    }

    if flow_steps == "all" or "link_dtr_to_regrid" in flow_steps_list:
        link_dtr_to_regrid(**link_dtr_kwargs)

    ### ERA5 DTR processing
    process_era5_dtr_kwargs = base_kwargs.copy()
    del process_era5_dtr_kwargs["variables"]
    del process_era5_dtr_kwargs["models"]
    del process_era5_dtr_kwargs["scenarios"]
    process_era5_dtr_kwargs.update(
        {
            "era5_dir": reference_dir,
        }
    )

    if flow_steps == "all" or "process_era5_dtr" in flow_steps_list:
        era5_dtr_dir = process_era5_dtr(**process_era5_dtr_kwargs)
    else:
        era5_dtr_dir = f"{scratch_dir}/{work_dir_name}/era5_dtr"

    era5_target_dtr_dir = reference_dir.joinpath("dtr")
    link_era5_dtr_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "src_dir": era5_dtr_dir,
        "target_dir": era5_target_dtr_dir,
    }

    if flow_steps == "all" or "link_dir" in flow_steps_list:
        link_dir(**link_era5_dtr_kwargs)

    ref_data_check_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "reference_dir": reference_dir,
        "scratch_dir": scratch_dir,
        "working_dir": working_dir,
    }

    if flow_steps == "all" or "ensure_reference_data_in_scratch" in flow_steps_list:
        reference_dir = ensure_reference_data_in_scratch(**ref_data_check_kwargs)
    else:
        reference_dir = f"{scratch_dir}/{work_dir_name}/ref_netcdf"

    ### convert ERA5 data to zarr
    convert_era5_to_zarr_kwargs = base_kwargs.copy()
    era5_vars = cmip6.cmip6_to_era5_variables(variables)
    convert_era5_to_zarr_kwargs.update(
        variables=era5_vars,
        netcdf_dir=reference_dir,
    )
    del convert_era5_to_zarr_kwargs["models"]
    del convert_era5_to_zarr_kwargs["scenarios"]

    if flow_steps == "all" or "convert_era5_to_zarr" in flow_steps_list:
        ref_zarr_dir = convert_era5_to_zarr(**convert_era5_to_zarr_kwargs)
    else:
        ref_zarr_dir = Path(f"{scratch_dir}/{work_dir_name}/era5_zarr")

    ### convert CMIP6 data to zarr
    convert_cmip6_to_zarr_kwargs = base_kwargs.copy()
    convert_cmip6_to_zarr_kwargs.update(
        netcdf_dir=final_regrid_dir,
    )

    if flow_steps == "all" or "convert_cmip6_to_zarr" in flow_steps_list:
        cmip6_zarr_dir = convert_cmip6_to_zarr(**convert_cmip6_to_zarr_kwargs)
    else:
        cmip6_zarr_dir = f"{scratch_dir}/{work_dir_name}/cmip6_zarr"

    time.sleep(1800)

    ### Train bias adjustment
    train_bias_adjust_kwargs = base_kwargs.copy()
    del train_bias_adjust_kwargs["scenarios"]
    train_bias_adjust_kwargs.update(
        {
            "sim_dir": cmip6_zarr_dir,
            "ref_dir": ref_zarr_dir,
        }
    )

    if flow_steps == "all" or "train_bias_adjustment" in flow_steps_list:
        train_dir = train_bias_adjustment(**train_bias_adjust_kwargs)
    else:
        train_dir = f"{scratch_dir}/{work_dir_name}/trained_datasets"

    time.sleep(1800)

    ### Bias adjustment (final step)
    bias_adjust_kwargs = base_kwargs.copy()
    bias_adjust_kwargs.update(
        {
            "sim_dir": cmip6_zarr_dir,
            "train_dir": train_dir,
        }
    )

    if flow_steps == "all" or "bias_adjustment" in flow_steps_list:
        adjusted_dir = bias_adjustment(**bias_adjust_kwargs)
    else:
        adjusted_dir = f"{scratch_dir}/{work_dir_name}/adjusted"

    derive_tasmin_kwargs = base_kwargs.copy()
    del derive_tasmin_kwargs["work_dir_name"]
    del derive_tasmin_kwargs["variables"]
    del derive_tasmin_kwargs["branch_name"]
    tasmin_output_dir = working_dir.joinpath("cmip6_tasmin")
    derive_tasmin_kwargs.update(
        {
            "input_dir": adjusted_dir,
            "output_dir": tasmin_output_dir,
            "slurm_dir": slurm_dir,
        }
    )

    if flow_steps == "all" or "derive_cmip6_tasmin" in flow_steps_list:
        derive_cmip6_tasmin(**derive_tasmin_kwargs)

    derive_era5_tasmin_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "era5_zarr_dir": ref_zarr_dir,
        "slurm_dir": slurm_dir,
        "repo_dir": scratch_dir.joinpath(repo_name),
        "conda_env_name": conda_env_name,
        "partition": partition,
    }

    if flow_steps == "all" or "derive_era5_tasmin" in flow_steps_list:
        derive_era5_tasmin(**derive_era5_tasmin_kwargs)


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

    # If not "all", specify any of these flow steps as a space-separated string:
    # - create_remote_directories
    # - clone_and_install_repo
    # - create_cascade_target_grid_file
    # - regrid_cmip6
    # - run_regrid_cmip6_again
    # - process_dtr
    # - link_dtr_to_regrid
    # - process_era5_dtr
    # - link_dir
    # - ensure_reference_data_in_scratch
    # - convert_era5_to_zarr
    # - convert_cmip6_to_zarr
    # - train_bias_adjustment
    # - bias_adjustment
    # - derive_cmip6_tasmin
    # - derive_era5_tasmin
    flow_steps = "all"

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
        "flow_steps": flow_steps,
    }
    downscale_cmip6.serve(
        name="downscale-cmip6",
        parameters=params_dict,
    )
