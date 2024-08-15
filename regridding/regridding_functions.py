from time import sleep
from prefect import task
import paramiko
from luts import *


@task
def check_for_nfs_mount(ssh, nfs_directory="/import/beegfs"):
    """
    Task to check if an NFS directory is mounted on the remote server via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - nfs_directory: Path to the NFS directory to check for
    """

    stdin, stdout, stderr = ssh.exec_command(f"df -h | grep {nfs_directory}")

    nfs_mounted = bool(stdout.read())

    if not nfs_mounted:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task
def clone_github_repository(ssh, branch, destination_directory):
    """
    Task to clone a GitHub repository via SSH and switch to a specific branch if it exists.

    Parameters:
    - ssh: Paramiko SSHClient object
    - branch: Name of the branch to clone and switch to
    - destination_directory: Directory to clone the repository into
    """

    target_directory = f"{destination_directory}/cmip6-utils"
    stdin, stdout, stderr = ssh.exec_command(
        f"if [ -d '{target_directory}' ]; then echo 'true'; else echo 'false'; fi"
    )

    directory_exists = stdout.read().decode("utf-8").strip() == "true"

    if directory_exists:
        try:
            # Directory exists, check the current branch
            get_current_branch_command = (
                f"cd {target_directory} && git pull && git branch --show-current"
            )
            stdin, stdout, stderr = ssh.exec_command(get_current_branch_command)
            current_branch = stdout.read().decode("utf-8").strip()
        except:
            # If the current branch cannot be determined, assume it's the wrong branch
            set_branch_to_main = f"cd {target_directory} && git checkout main"
            stdin, stdout, stderr = ssh.exec_command(set_branch_to_main)

            # Get the current branch again# Directory exists, check the current branch
            get_current_branch_command = (
                f"cd {target_directory} && git pull && git branch --show-current"
            )
            stdin, stdout, stderr = ssh.exec_command(get_current_branch_command)
            current_branch = stdout.read().decode("utf-8").strip()

        if current_branch != branch:
            print(f"Change repository branch to branch {branch}...")
            # If the current branch is different from the desired branch, switch to the correct branch
            switch_branch_command = f"cd {target_directory} && git checkout {branch}"
            stdin, stdout, stderr = ssh.exec_command(switch_branch_command)

        print(f"Pulling the GitHub repository on branch {branch}...")

        # Run the Git pull command to pull the repository
        git_pull_command = f"cd {target_directory} && git pull origin {branch}"
        stdin, stdout, stderr = ssh.exec_command(git_pull_command)

        # Wait for the Git command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        # Check the exit status for errors
        if exit_status != 0:
            raise Exception(
                f"Error cloning the GitHub repository. Exit status: {exit_status}"
            )
    else:
        print(f"Cloning the GitHub repository on branch {branch}...")
        # Run the Git clone command to clone the repository
        git_command = f"cd {destination_directory} && git clone -b {branch} https://github.com/ua-snap/cmip6-utils.git"
        stdin, stdout, stderr = ssh.exec_command(git_command)

        # Wait for the Git command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        # Check the exit status for errors
        if exit_status != 0:
            error_output = stderr.read().decode("utf-8")
            raise Exception(
                f"Error cloning the GitHub repository. Error: {error_output}"
            )


@task
def install_conda_environment(ssh, conda_env_name, conda_env_file):
    """
    Task to check for a Python Conda environment and install it from an environment file
    if it doesn't exist on the user's account via SSH. It also checks for Miniconda installation
    and installs Miniconda if it doesn't exist.

    Parameters:
    - ssh: Paramiko SSHClient object
    - conda_env_name: Name of the Conda environment to create/install
    - conda_env_file: Path to the Conda environment file (.yml) to use for installation
    """

    # Check if the Miniconda directory exists in the user's home directory
    stdin, stdout, stderr = ssh.exec_command(
        "test -d $HOME/miniconda3 && echo 1 || echo 0"
    )

    miniconda_found = int(stdout.read())
    miniconda_installed = bool(miniconda_found)

    if not miniconda_installed:
        print("Miniconda directory not found. Installing Miniconda...")
        # Download and install Miniconda
        install_miniconda_cmd = "wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && bash miniconda.sh -b -p $HOME/miniconda3"
        stdin, stdout, stderr = ssh.exec_command(install_miniconda_cmd)

        # Wait for the command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            error_output = stderr.read().decode("utf-8")
            raise Exception(f"Error installing Miniconda. Error: {error_output}")

        print("Miniconda installed successfully")

    # Check if the Conda environment already exists
    stdin, stdout, stderr = ssh.exec_command(
        f"source $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env list | grep {conda_env_name}"
    )

    conda_env_exists = bool(stdout.read())

    if not conda_env_exists:
        print(f"Conda environment '{conda_env_name}' does not exist. Installing...")

        # Install the Conda environment from the environment file
        install_cmd = f"source $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env create -n {conda_env_name} -f {conda_env_file}"
        stdin, stdout, stderr = ssh.exec_command(install_cmd)

        # Wait for the command to finish and get the exit status
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            print(f"Conda environment '{conda_env_name}' installed successfully")
        else:
            error_output = stderr.read().decode("utf-8")
            raise Exception(
                f"Error installing Conda environment '{conda_env_name}'. Error: {error_output}"
            )
    else:
        print(f"Conda environment '{conda_env_name}' already exists.")


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
def get_job_ids(ssh, username):
    """
    Task to get a list of job IDs for a specified user from the Slurm queue via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - username: Username to get job IDs for
    """

    stdin_, stdout, stderr_ = ssh.exec_command(
        f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin && squeue -u {username}"
    )

    # Get a list of job IDs for the specified user
    job_ids = [line.split()[0] for line in stdout.readlines()[1:]]  # Skip header

    # Prints the list of job IDs to the log for debugging purposes
    print(job_ids)
    return job_ids


@task
def wait_for_jobs_completion(ssh, job_ids):
    """
    Task to wait for a list of Slurm jobs to complete in the queue via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - job_ids: List of job IDs to wait for
    """

    while job_ids:
        # Check the status of each job in the list
        for job_id in job_ids.copy():
            stdin_, stdout, stderr_ = ssh.exec_command(
                f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin && squeue -h -j {job_id}"
            )

            # If the job is no longer in the queue, remove it from the list
            if not stdout.read():
                job_ids.remove(job_id)

        if job_ids:
            # Sleep for a while before checking again
            sleep(10)

    print("Jobs completed!")


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
