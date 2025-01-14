from time import sleep
from prefect import task
import paramiko
from utils import utils
from luts import *


@task
def run_generate_batch_files(
    ssh,
    conda_init_script,
    conda_env_name,
    generate_batch_files_script,
    run_generate_batch_files_script,
    cmip6_directory,
    regrid_batch_dir,
    vars,
    freqs,
    models,
    scenarios,
):
    """
    Task to create and submit slurm script to generate the batch files for the regridding.

    Parameters:
    - ssh: Paramiko SSHClient object
    - conda_init_script: Script to initialize conda during slurm jobs
    - conda_env_name: Name of the Conda environment to activate for processing
    - generate_batch_files_script: Location of the script to generate batch files
    - run_generate_batch_files_script: Location of the script to run the batch file generation script
    - cmip6_directory: Path to the CMIP6 data directory
    - regrid_batch_dir: Directory to save the batch files
    - vars: Variables to regrid
    - freqs: Frequencies to regrid
    - models: Models to regrid
    - scenarios: Scenarios to regrid
    """
    cmd = (
        f"python {run_generate_batch_files_script}"
        f" --generate_batch_files_script {generate_batch_files_script}"
        f" --conda_init_script {conda_init_script}"
        f" --conda_env_name {conda_env_name}"
        f" --cmip6_directory {cmip6_directory}"
        f" --regrid_batch_dir {regrid_batch_dir}"
        f" --vars '{vars}' --freqs '{freqs}' --models '{models}' --scenarios '{scenarios}'"
    )
    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    # Check the exit status for errors
    if exit_status != 0:
        raise Exception(f"Error generating batch files. Error: {stderr}")

    job_ids = utils.parse_job_ids(stdout)
    assert (
        len(job_ids) == 1
    ), f"More than one job ID given for batch file generation: {job_ids}"

    print(f"Generate batch files job submitted! (job ID: {job_ids[0]})")

    return job_ids


@task
def run_regridding(
    ssh,
    slurm_script,
    slurm_dir,
    regrid_dir,
    regrid_batch_dir,
    conda_init_script,
    conda_env_name,
    regrid_script,
    target_grid_fp,
    no_clobber,
    vars,
    interp_method,
    freqs,
    models,
    scenarios,
    target_sftlf_fp=None,
):
    """
    Task to create and submit Slurm scripts to regrid batches of CMIP6 data.

    Parameters:
    - ssh: Paramiko SSHClient object
    - slurm_script: Directory to regridding slurm.py script
    - slurm_dir: Directory to save slurm sbatch files
    - regrid_dir: Path to directory where regridded files are written
    - regrid_batch_dir: Directory of batch files
    - conda_init_script: Script to initialize conda during slurm jobs
    - conda_env_name: Name of the Conda environment to activate
    - regrid_script: Location of regrid.py script in the repo
    - target_grid_fp: Path to file used as the regridding target
    - no_clobber: Do not overwrite regridded files if they exist
    - vars: Variables to regrid
    - interp_method: Interpolation method to use
    - freqs: Frequencies to regrid
    - models: Models to regrid
    - scenarios: Scenarios to regrid
    """

    cmd = (
        f"python {slurm_script}"
        f" --slurm_dir {slurm_dir}"
        f" --regrid_dir {regrid_dir}"
        f" --regrid_batch_dir {regrid_batch_dir}"
        f" --conda_init_script {conda_init_script}"
        f" --conda_env_name {conda_env_name}"
        f" --regrid_script {regrid_script}"
        f" --target_grid_fp {target_grid_fp}"
        f" --interp_method {interp_method}"
        f" --vars '{vars}' --freqs '{freqs}' --models '{models}' --scenarios '{scenarios}'"
    )

    if target_sftlf_fp:
        cmd += f" --target_sftlf_fp {target_sftlf_fp}"

    if no_clobber:
        cmd += " --no_clobber"

    exit_status, stdout, stderr = utils.exec_command(ssh, cmd)

    # Check the exit status for errors
    if exit_status != 0:
        raise Exception(
            f"Error creating and submitting regridding slurm scripts. Error: {stderr}"
        )

    job_ids = utils.parse_job_ids(stdout)

    print(f"Regridding jobs submitted! (job IDs: {job_ids})")

    return job_ids


@task
def validate_vars(vars):
    """
    Task to validate strings of variables. Variables are checked against the lists in luts.py.
    Parameters:
    - vars: a string of variable ids separated by white space (e.g., 'pr tas ta') or variable group names found in luts.py (e.g. 'land')
    """
    if vars == "all":
        return (" ").join(all_vars)
    elif vars == "land":
        return (" ").join(land_vars)
    elif vars == "sea":
        return (" ").join(sea_vars)
    elif vars == "global":
        return (" ").join(global_vars)
    else:
        var_list = vars.split()
        assert all(x in all_vars for x in var_list), "Variables not valid."
        return vars


@task
def validate_freqs(freq_str):
    """
    Task to validate frequencies to work on.
    Parameters:
    - freqs_str: a string of variable ids separated by white space (e.g., 'pr tas ta') or variable group names found in luts.py (e.g. 'land')
    """

    if freq_str == "all":
        return (" ").join(all_freqs)
    else:
        freqs = freq_str.split()
        assert all(x in all_freqs for x in freqs), "Variables not valid."
        return freq_str


@task
def validate_models(models_str):
    """Task to validate string of models to work on.
    Parameters:
    - models_str: string of models separated by white space (e.g., 'CESM2 GFDL-ESM4')
    """
    if models_str == "all":
        return (" ").join(all_models)
    else:
        models = models_str.split()
        assert all(x in all_models for x in models), "Models not valid."
        return models_str


@task
def validate_scenarios(scenarios_str):
    """Task to validate string of scenarios to work on.
    Parameters:
    - scenarios_str: string of scenarios separated by white space (e.g., 'historical ssp585')
    """
    if scenarios_str == "all":
        return (" ").join(all_scenarios)
    else:
        scenarios = scenarios_str.split()
        assert all(x in all_scenarios for x in scenarios), "Scenarios not valid."
        return scenarios_str


@task
def run_qc(
    ssh,
    output_directory,
    cmip6_directory,
    repo_regridding_directory,
    conda_init_script,
    conda_env_name,
    run_qc_script,
    qc_notebook,
    vars,
    freqs,
    models,
    scenarios,
):

    exit_status, stdout, stderr = utils.exec_command(
        ssh,
        (
            f"python {run_qc_script}"
            f" --qc_notebook '{qc_notebook}'"
            f" --conda_init_script '{conda_init_script}' --conda_env_name '{conda_env_name}'"
            f" --cmip6_directory '{cmip6_directory}' --output_directory '{output_directory}'"
            f" --repo_regridding_directory '{repo_regridding_directory}'"
            f" --vars '{vars}' --freqs '{freqs}' --models '{models}' --scenarios '{scenarios}'"
        ),
    )

    # Check the exit status for errors
    if exit_status != 0:
        raise Exception(f"Error submitting QC scripts. Error: {stderr}")

    job_ids = utils.parse_job_ids(stdout)
    print(f"QC jobs submitted! (job IDs: {job_ids[0]})")

    return job_ids
