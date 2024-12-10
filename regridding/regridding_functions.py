from time import sleep
from prefect import task
import paramiko
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
    - slurm_script: Directory to regridding slurm.py script
    - slurm_dir: Directory to save slurm sbatch files
    - regrid_dir: Path to directory where regridded files are written
    - regrid_batch_dir: Directory of batch files
    - conda_init_script: Script to initialize conda during slurm jobs
    - conda_env_name: Name of the Conda environment to activate
    - regrid_script: Location of regrid.py script in the repo
    - target_grid_fp: Path to file used as the regridding target
    - no_clobber: Do not overwrite regridded files if they exist
    """
    stdin_, stdout, stderr = ssh.exec_command(
        f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin:$HOME/miniconda3/bin && python {run_generate_batch_files_script} --generate_batch_files_script '{generate_batch_files_script}' --conda_init_script '{conda_init_script}' --conda_env_name {conda_env_name} --cmip6_directory '{cmip6_directory}' --regrid_batch_dir '{regrid_batch_dir}' --vars '{vars}' --freqs '{freqs}' --models '{models}' --scenarios '{scenarios}'"
    )

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    # Check the exit status for errors
    if exit_status != 0:
        error_output = stderr.read().decode("utf-8")
        raise Exception(f"Error generating batch files. Error: {error_output}")

    print("Generate batch files job submitted!")


@task
def create_and_run_slurm_scripts(
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
    freqs,
    models,
    scenarios,
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
    """

    cmd = f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin:$HOME/miniconda3/bin && python {slurm_script} --slurm_dir '{slurm_dir}' --regrid_dir '{regrid_dir}'  --regrid_batch_dir '{regrid_batch_dir}' --conda_init_script '{conda_init_script}' --conda_env_name {conda_env_name} --regrid_script '{regrid_script}' --target_grid_fp '{target_grid_fp}' --vars '{vars}' --freqs '{freqs}' --models '{models}' --scenarios '{scenarios}'"

    if no_clobber:
        cmd += " --no_clobber"

    stdin_, stdout, stderr = ssh.exec_command(cmd)

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    # Check the exit status for errors
    if exit_status != 0:
        error_output = stderr.read().decode("utf-8")
        raise Exception(
            f"Error creating or running Slurm scripts. Error: {error_output}"
        )

    print("Regridding jobs submitted!")


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
    qc_script,
    visual_qc_notebook,
    vars,
    freqs,
    models,
    scenarios,
):

    stdin_, stdout, stderr = ssh.exec_command(
        f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin:$HOME/miniconda3/bin && python {run_qc_script} --qc_script '{qc_script}' --visual_qc_notebook '{visual_qc_notebook}' --conda_init_script '{conda_init_script}' --conda_env_name '{conda_env_name}' --cmip6_directory '{cmip6_directory}' --output_directory '{output_directory}' --repo_regridding_directory '{repo_regridding_directory}' --vars '{vars}' --freqs '{freqs}' --models '{models}' --scenarios '{scenarios}'"
    )

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    # Check the exit status for errors
    if exit_status != 0:
        error_output = stderr.read().decode("utf-8")
        raise Exception(f"Error submitting QC scripts. Error: {error_output}")

    print("QC jobs submitted!")
