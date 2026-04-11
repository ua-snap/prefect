"""Flow for statistical downscaling CMIP6 data using the cmip6-utils repository.

Notes:
This flow is designed to be run on the Chinook cluster.

The flow consists of the following steps:
1. Create remote working and slurm directories.
2. Clone and install the cmip6-utils repo on the remote.
3. Generate batch files listing CMIP6 files to regrid, grouped by model/scenario/variable/grid.
4. Compute DTR from raw CMIP6 tasmax and tasmin (if dtr or tasmin requested).
5. Generate batch files for DTR data (if dtr or tasmin requested).
6. Create the intermediate target grid file for the first regridding step.
7. Regrid CMIP6 data to the intermediate grid (bilinear).
8. Create the second intermediate target grid file.
9. Regrid from the intermediate grid toward the final resolution.
10. Regrid to the final 4km target grid.
11. Ensure ERA5 reference data is in scratch space (copy if not).
12. Process DTR from the ERA5 data (if dtr or tasmin requested).
13. Convert ERA5 data to Zarr format.
14. Convert the regridded CMIP6 data to Zarr format.
15. Train bias adjustment model using historical data only. Weights/adjustment factors are saved on a per-model, per-variable basis.
16. Apply bias adjustment to the regridded CMIP6 data.
17. Derive tasmin from adjusted tasmax minus adjusted dtr (if tasmin requested).
"""

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
from regridding import regridding_functions as rf
# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22

# name of folder in working_dir where downscaled data is written
out_dir_name = "downscaled"


def get_batch_file_variables(variables):
    """Get variables needed for batch file generation.

    For DTR calculation, we need raw tasmin and tasmax from CMIP6 data.
    This function ensures those variables are included in batch files.

    Parameters:
        variables (str): String representation of variables list

    Returns:
        str: String representation of variables list for batch files
    """
    var_list = cmip6.validate_vars(variables, return_list=True)
    batch_variables_list = var_list.copy()

    # For DTR calculation or tasmin derivation, ensure we have source variables
    if "dtr" in var_list or "tasmin" in var_list:
        # Need raw tasmin and tasmax for DTR calculation
        if "tasmin" not in batch_variables_list:
            batch_variables_list.append("tasmin")
        if "tasmax" not in batch_variables_list:
            batch_variables_list.append("tasmax")

    batch_variables = " ".join(batch_variables_list)
    return batch_variables


def get_regrid_variables(variables):
    """Get variables that should actually be regridded.

    Includes tasmin when requested so it is available in cmip6_zarr/ for QC.
    Note: tasmin is also derived from bias-adjusted tasmax - dtr at the end
    of the pipeline; regridding it here provides the pre-bias-adjustment
    version for QC notebook use only.

    Parameters:
        variables (str): String representation of variables list

    Returns:
        str: String representation of variables list for regridding
    """
    var_list = cmip6.validate_vars(variables, return_list=True)
    regrid_variables_list = var_list.copy()

    # Ensure tasmax is present when dtr or tasmin is requested,
    # since the bias adjustment pipeline needs tasmax alongside these variables.
    if "dtr" in var_list or "tasmin" in var_list:
        if "tasmax" not in regrid_variables_list:
            regrid_variables_list.append("tasmax")

    regrid_variables = " ".join(regrid_variables_list)
    return regrid_variables


def get_zarr_conversion_variables(variables):
    """Get variables for the CMIP6 zarr conversion step.

    Unlike get_processing_variables(), this includes tasmin when requested
    so it is available in cmip6_zarr/ for QC purposes in notebooks.
    Tasmin files must exist in the regridded source directory (i.e.,
    get_regrid_variables() must also include tasmin) for this to work.

    Parameters:
        variables (str): String representation of variables list

    Returns:
        str: String representation of variables list for zarr conversion
    """
    var_list = cmip6.validate_vars(variables, return_list=True)
    conversion_list = var_list.copy()

    # Ensure tasmax is present for the bias adjustment pipeline when
    # dtr or tasmin is involved.
    if "dtr" in var_list or "tasmin" in var_list:
        if "tasmax" not in conversion_list:
            conversion_list.append("tasmax")

    return " ".join(conversion_list)


def get_processing_variables(variables):
    """Get variables that should be processed through zarr/train/bias_adjust pipeline.

    Excludes tasmin if user requested it, since it will be derived from tasmax - dtr.

    Parameters:
        variables (str): String representation of variables list

    Returns:
        str: String representation of variables list for processing
    """
    var_list = cmip6.validate_vars(variables, return_list=True)

    # Exclude tasmin if user requested it (we'll derive it later)
    processing_list = [v for v in var_list if v != "tasmin"]

    # For deriving tasmin, we need tasmax and dtr to be processed
    if "tasmin" in var_list:
        if "tasmax" not in processing_list:
            processing_list.append("tasmax")
        if "dtr" not in processing_list:
            processing_list.append("dtr")

    processing_variables = " ".join(processing_list)
    return processing_variables


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
def prune_empty_directories(
    ssh_username,
    ssh_private_key_path,
    base_dir,
):
    """Remove empty scenario directories from DTR output.

    Some models don't have data for all scenarios, but the DTR script creates
    directory structures for all scenarios anyway. This removes empty ones.

    Parameters
    ----------
    base_dir : Path or str
        Base directory containing model/scenario/frequency/variable structure
    """
    logger = get_run_logger()
    logger.info(f"Pruning empty directories from {base_dir}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        # Find all scenario directories (they're at depth 2: model/scenario/)
        # Check if they contain any files recursively
        cmd = f"""
        cd {base_dir} 2>/dev/null || exit 0
        for model_dir in */; do
            [ -d "$model_dir" ] || continue
            cd "$model_dir"
            for scenario_dir in */; do
                [ -d "$scenario_dir" ] || continue
                # Count files in scenario directory tree
                file_count=$(find "$scenario_dir" -type f 2>/dev/null | wc -l)
                if [ "$file_count" -eq 0 ]; then
                    echo "Removing empty: $model_dir$scenario_dir"
                    rm -rf "$scenario_dir"
                fi
            done
            cd ..
        done
        """

        exit_status, stdout, stderr = utils.exec_command(ssh, cmd)
        if stdout:
            logger.info(f"Pruned directories:\n{stdout}")
        if exit_status != 0 and stderr:
            logger.warning(f"Warning during pruning: {stderr}")

    finally:
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
def create_first_regrid_target_file(
    ssh_username,
    ssh_private_key_path,
    cascade_grid_script,
    first_regrid_source_file,
    scratch_dir,
    work_dir_name,
    step,
    resolution,
):
    first_regrid_target_file = scratch_dir.joinpath(
        work_dir_name, "first_regrid_target_file.nc"
    )

    logger = get_run_logger()
    logger.info(f"Creating target grid file {first_regrid_target_file}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # /center1/CMIP6/kmredilla/cmip6_4km_downscaling/first_regrid_target_file.nc
    cmd = f"conda activate cmip6-utils && \
            python {cascade_grid_script} \
            --src_file {first_regrid_source_file} \
            --out_file {first_regrid_target_file} \
            --step {step} \
            --resolution {resolution}"
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

    return first_regrid_target_file


@task
def create_second_regrid_target_file(
    ssh_username,
    ssh_private_key_path,
    cascade_grid_script,
    second_regrid_source_file,
    scratch_dir,
    work_dir_name,
    step,
    resolution,
):
    second_regrid_target_file = scratch_dir.joinpath(
        work_dir_name, "second_regrid_target_file.nc"
    )

    logger = get_run_logger()
    logger.info(f"Creating target grid file {second_regrid_target_file}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # /center1/CMIP6/kmredilla/cmip6_4km_downscaling/second_regrid_target_file.nc
    cmd = f"conda activate cmip6-utils && \
            python {cascade_grid_script} \
            --src_file {second_regrid_source_file} \
            --out_file {second_regrid_target_file} \
            --step {step} \
            --resolution {resolution}"
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

    return second_regrid_target_file


@flow
# putting this flow here now because it has more to do with downscaling than general regridding
def another_cmip6_regrid(
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
    stage,
):
    """Flow for regridding CMIP6 data that has been regridded once and so is all on a common grid.

    Parameters
    ----------
    stage : str
        Regridding stage identifier ('second' or 'final')
    """
    logger = get_run_logger()
    logger.info(f"Regridding CMIP6 data ({stage} stage) to {target_grid_file}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    slurm_dir = working_dir.joinpath("slurm")
    # Deprecated: regrid_again_batch_dir is now created within stage-specific subdirectory
    regrid_again_batch_dir = slurm_dir.joinpath(
        "regrid_again_batch"
    )  # Keep for backward compatibility in args
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
        f"--stage {stage} "
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
            f"CMIP6 regridding ({stage} stage) job submitted! (job ID: {job_ids[0]})"
        )

        # Find the sbatch script for potential retry in stage-specific subdirectory
        slurm_subdir = slurm_dir.joinpath(f"{stage}_regrid")
        sbatch_script = slurm_subdir.joinpath(f"regrid_{stage}.slurm")

        # Use retry logic to handle intermittent 0:53 errors
        final_job_ids = utils.wait_for_jobs_with_retry(
            ssh,
            job_ids,
            sbatch_script_path=sbatch_script if sbatch_script.exists() else None,
            max_job_retries=3,
            retry_delay=60,
            exponential_backoff=True,
            completion_message=f"Slurm jobs for {stage} regridding complete.",
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

        # Use retry logic to handle intermittent 0:53 errors
        derive_slurm_subdir = Path(slurm_dir) / "derive_tasmin"
        derive_sbatch_script = derive_slurm_subdir / "process_cmip6_diff_tasmin.slurm"

        final_job_ids = utils.wait_for_jobs_with_retry(
            ssh,
            job_ids,
            sbatch_script_path=(
                derive_sbatch_script if derive_sbatch_script.exists() else None
            ),
            max_job_retries=5,
            retry_delay=60,
            exponential_backoff=True,
            logger=logger,
            completion_message="Slurm job for deriving CMIP6 tasmin complete.",
        )

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
    first_regrid_linspace_step,
    second_regrid_linspace_step,
    resolution,
):
    logger = get_run_logger()

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

    # Expand "all" shorthand once so all steps receive explicit model/scenario lists
    if models == "all":
        models = " ".join(cmip6.all_models)
    if scenarios == "all":
        scenarios = " ".join(cmip6.all_scenarios)

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

    if flow_steps == "all" or "generate_batch_files" in flow_steps_list:
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

            utils.check_for_nfs_mount(ssh, "/import/beegfs")

            utils.ensure_slurm(ssh)

            utils.ensure_conda(ssh)

            utils.ensure_conda_env(
                ssh, conda_env_name, repo_path.joinpath("environment.yml")
            )

            generate_batch_files_script = (
                f"{scratch_dir}/cmip6-utils/regridding/generate_batch_files.py"
            )
            run_generate_batch_files_script = (
                f"{scratch_dir}/cmip6-utils/regridding/run_generate_batch_files.py"
            )

            freqs = "day"
            batch_file_kwargs = {
                "ssh": ssh,
                "conda_env_name": conda_env_name,
                "generate_batch_files_script": generate_batch_files_script,
                "run_generate_batch_files_script": run_generate_batch_files_script,
                "cmip6_dir": cmip6_dir,
                "slurm_dir": slurm_dir,
                "vars": get_batch_file_variables(variables),
                "freqs": freqs,
                "models": models,
                "scenarios": scenarios,
            }
            batch_job_ids = rf.run_generate_batch_files(**batch_file_kwargs)

            utils.wait_for_jobs_completion(
                ssh,
                batch_job_ids,
                completion_message="Slurm jobs for batch file generation complete.",
                logger=logger,
            )
        finally:
            ssh.close()

    batch_files_dir = f"{scratch_dir}/{work_dir_name}/slurm/first_regrid/batch"

    # Check if DTR processing is needed
    var_list = cmip6.validate_vars(variables, return_list=True)
    needs_dtr = "dtr" in var_list
    needs_era5_dtr = "dtr" in var_list or "tasmin" in var_list

    ### CMIP6 DTR processing
    process_dtr_kwargs = base_kwargs.copy()
    del process_dtr_kwargs["variables"]
    process_dtr_kwargs.update(
        {
            "input_dir": batch_files_dir,
        }
    )

    if needs_dtr and (flow_steps == "all" or "process_dtr" in flow_steps_list):
        cmip6_dtr_dir = process_dtr(**process_dtr_kwargs)
        # Remove empty scenario directories (some models don't have all scenarios)
        prune_empty_directories(
            ssh_username=ssh_username,
            ssh_private_key_path=ssh_private_key_path,
            base_dir=cmip6_dtr_dir,
        )
    else:
        cmip6_dtr_dir = f"{scratch_dir}/{work_dir_name}/cmip6_dtr"

    ### Prep DTR data for regridding by adding DTR files to batch files for regridding step
    if needs_dtr:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        generate_batch_files_script = (
            f"{scratch_dir}/cmip6-utils/regridding/generate_batch_files.py"
        )
        run_generate_batch_files_script = (
            f"{scratch_dir}/cmip6-utils/regridding/run_generate_batch_files.py"
        )

        # Add DTR files to batch files for regridding
        freqs = "day"
        batch_file_kwargs = {
            "ssh": ssh,
            "conda_env_name": conda_env_name,
            "generate_batch_files_script": generate_batch_files_script,
            "run_generate_batch_files_script": run_generate_batch_files_script,
            "cmip6_dir": cmip6_dtr_dir,
            "slurm_dir": slurm_dir,
            "vars": "dtr",
            "freqs": freqs,
            "models": models,
            "scenarios": scenarios,
        }
        batch_job_ids = rf.run_generate_batch_files(**batch_file_kwargs)

        utils.wait_for_jobs_completion(
            ssh,
            batch_job_ids,
            completion_message="Slurm jobs for batch file generation complete.",
            logger=logger,
        )

    ### Regridding 1: Regrid CMIP6 data to intermediate grid
    # first, run the task to create the intermediate target grid file
    cascade_grid_script = scratch_dir.joinpath(
        repo_name, "downscaling", "make_intermediate_target_grid_file.py"
    )

    first_regrid_source_file = cmip6_dir.joinpath(
        "ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/Amon/tas/gn/v20200528/tas_Amon_CESM2_ssp370_r11i1p1f1_gn_206501-210012.nc"
    )

    first_regrid_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "cascade_grid_script": cascade_grid_script,
        "first_regrid_source_file": first_regrid_source_file,
        "scratch_dir": scratch_dir,
        "work_dir_name": work_dir_name,
        "step": first_regrid_linspace_step,
        "resolution": resolution,
    }

    if flow_steps == "all" or "create_first_regrid_target_file" in flow_steps_list:
        first_cascade_target_file = create_first_regrid_target_file(
            **first_regrid_kwargs
        )
    else:
        first_cascade_target_file = (
            f"{scratch_dir}/{work_dir_name}/first_regrid_target_file.nc"
        )

    first_regrid_out_dir_name = "first_regrid"
    first_regrid_kwargs = base_kwargs.copy()
    regrid_variables = get_regrid_variables(variables)
    interp_method = "bilinear"
    first_regrid_kwargs.update(
        {
            "cmip6_dir": cmip6_dir,
            "target_grid_file": first_cascade_target_file,
            "interp_method": interp_method,
            "out_dir_name": first_regrid_out_dir_name,
            "freqs": "day",
            "rasdafy": False,
            "no_clobber": False,
            "variables": regrid_variables,
        }
    )

    if flow_steps == "all" or "first_cmip6_regrid" in flow_steps_list:
        first_regrid_dir = regrid_cmip6(**first_regrid_kwargs)
    else:
        first_regrid_dir = f"{scratch_dir}/{work_dir_name}/{first_regrid_out_dir_name}"

    second_regrid_source_file = cmip6_dir.joinpath(
        "ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/Amon/tas/gn/v20200528/tas_Amon_CESM2_ssp370_r11i1p1f1_gn_206501-210012.nc"
    )

    second_regrid_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "cascade_grid_script": cascade_grid_script,
        "second_regrid_source_file": second_regrid_source_file,
        "scratch_dir": scratch_dir,
        "work_dir_name": work_dir_name,
        "step": second_regrid_linspace_step,
        "resolution": resolution,
    }

    if flow_steps == "all" or "create_second_regrid_target_file" in flow_steps_list:
        second_cascade_target_file = create_second_regrid_target_file(
            **second_regrid_kwargs
        )
    else:
        second_cascade_target_file = (
            f"{scratch_dir}/{work_dir_name}/second_regrid_target_file.nc"
        )

    regrid_again_script = scratch_dir.joinpath(
        repo_name, "regridding", "run_regrid_again.py"
    )
    regrid_script = scratch_dir.joinpath(repo_name, "regridding", "regrid.py")

    second_regrid_out_dir_name = "second_regrid"
    second_regrid_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "conda_env_name": conda_env_name,
        "partition": partition,
        "launcher_script": regrid_again_script,
        "regrid_script": regrid_script,
        "interp_method": interp_method,
        "target_grid_file": second_cascade_target_file,
        "working_dir": working_dir,
        "regridded_dir": first_regrid_dir,
        "out_dir_name": second_regrid_out_dir_name,
        "stage": "second",
    }

    if flow_steps == "all" or "second_cmip6_regrid" in flow_steps_list:
        second_regrid_dir = another_cmip6_regrid(**second_regrid_kwargs)
    else:
        second_regrid_dir = (
            f"{scratch_dir}/{work_dir_name}/{second_regrid_out_dir_name}"
        )

    # TO-DO: take target grid file from the reference data, e.g.:
    # target_grid_source_file = reference_dir.joinpath(
    #     "t2max/t2max_2014_era5_4km_3338.nc"
    # )

    final_regrid_out_dir_name = "final_regrid"
    final_regrid_kwargs = {
        "ssh_username": ssh_username,
        "ssh_private_key_path": ssh_private_key_path,
        "conda_env_name": conda_env_name,
        "partition": partition,
        "launcher_script": regrid_again_script,
        "regrid_script": regrid_script,
        "interp_method": interp_method,
        "target_grid_file": target_grid_source_file,
        "working_dir": working_dir,
        "regridded_dir": second_regrid_dir,
        "out_dir_name": final_regrid_out_dir_name,
        "stage": "final",
    }

    if flow_steps == "all" or "final_cmip6_regrid" in flow_steps_list:
        final_regrid_dir = another_cmip6_regrid(**final_regrid_kwargs)
    else:
        final_regrid_dir = f"{scratch_dir}/{work_dir_name}/final_regrid"

    ### Ensure reference data is in scratch space FIRST (before creating symlinks)
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

    ### ERA5 DTR processing
    process_era5_dtr_kwargs = base_kwargs.copy()
    del process_era5_dtr_kwargs["variables"]
    del process_era5_dtr_kwargs["models"]
    del process_era5_dtr_kwargs["scenarios"]
    process_era5_dtr_kwargs.update(
        {
            "era5_dir": reference_dir,
            "resolution": resolution,
        }
    )

    if needs_era5_dtr and (
        flow_steps == "all" or "process_era5_dtr" in flow_steps_list
    ):
        era5_dtr_dir = process_era5_dtr(**process_era5_dtr_kwargs)
    else:
        era5_dtr_dir = f"{scratch_dir}/{work_dir_name}/era5_dtr"

    # Note: ERA5 DTR processing writes directly to ref_netcdf/dtr, no linking needed

    ### convert ERA5 data to zarr
    convert_era5_to_zarr_kwargs = base_kwargs.copy()
    processing_vars = get_processing_variables(variables)
    # For ERA5, include t2min whenever dtr, tasmax, or tasmin are involved,
    # since ERA5 t2min exists directly and is needed as reference data.
    var_list_original = cmip6.validate_vars(variables, return_list=True)
    era5_var_list = cmip6.cmip6_to_era5_variables(processing_vars).split()
    if any(v in var_list_original for v in ["dtr", "tasmax", "tasmin"]):
        if "t2min" not in era5_var_list:
            era5_var_list.append("t2min")
    era5_vars = " ".join(era5_var_list)
    convert_era5_to_zarr_kwargs.update(
        variables=era5_vars,
        netcdf_dir=reference_dir,
        resolution=resolution,
    )
    del convert_era5_to_zarr_kwargs["models"]
    del convert_era5_to_zarr_kwargs["scenarios"]

    if flow_steps == "all" or "convert_era5_to_zarr" in flow_steps_list:
        ref_zarr_dir = convert_era5_to_zarr(**convert_era5_to_zarr_kwargs)
    else:
        ref_zarr_dir = Path(f"{scratch_dir}/{work_dir_name}/era5_zarr")

    ### convert CMIP6 data to zarr
    convert_cmip6_to_zarr_kwargs = base_kwargs.copy()
    conversion_vars = get_zarr_conversion_variables(variables)
    convert_cmip6_to_zarr_kwargs["variables"] = conversion_vars
    convert_cmip6_to_zarr_kwargs.update(
        netcdf_dir=final_regrid_dir,
    )

    if flow_steps == "all" or "convert_cmip6_to_zarr" in flow_steps_list:
        cmip6_zarr_dir = convert_cmip6_to_zarr(**convert_cmip6_to_zarr_kwargs)
    else:
        cmip6_zarr_dir = f"{scratch_dir}/{work_dir_name}/cmip6_zarr"

    ### Train bias adjustment
    train_bias_adjust_kwargs = base_kwargs.copy()
    processing_vars = get_processing_variables(variables)
    train_bias_adjust_kwargs["variables"] = processing_vars
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

    ### Bias adjustment (final step)
    bias_adjust_kwargs = base_kwargs.copy()
    processing_vars = get_processing_variables(variables)
    bias_adjust_kwargs["variables"] = processing_vars
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
    tasmin_output_dir = adjusted_dir
    derive_tasmin_kwargs.update(
        {
            "input_dir": adjusted_dir,
            "output_dir": tasmin_output_dir,
            "slurm_dir": slurm_dir,
        }
    )

    needs_tasmin_derivation = "tasmin" in var_list
    if needs_tasmin_derivation and (
        flow_steps == "all" or "derive_cmip6_tasmin" in flow_steps_list
    ):
        derive_cmip6_tasmin(**derive_tasmin_kwargs)


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
    first_regrid_linspace_step = 0.5
    second_regrid_linspace_step = 0.25
    resolution = 4

    # If not "all", specify any of these flow steps as a space-separated string:
    # - create_remote_directories
    # - clone_and_install_repo
    # - generate_batch_files
    # - process_dtr
    # - create_first_regrid_target_file
    # - first_cmip6_regrid
    # - create_second_regrid_target_file
    # - second_cmip6_regrid
    # - final_cmip6_regrid
    # - process_era5_dtr
    # - ensure_reference_data_in_scratch
    # - convert_era5_to_zarr
    # - convert_cmip6_to_zarr
    # - train_bias_adjustment
    # - bias_adjustment
    # - derive_cmip6_tasmin
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
        "first_regrid_linspace_step": first_regrid_linspace_step,
        "second_regrid_linspace_step": second_regrid_linspace_step,
        "resolution": resolution,
    }
    downscale_cmip6.serve(
        name="downscale-cmip6",
        parameters=params_dict,
    )
