from time import sleep
from prefect import task
import paramiko


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
def create_and_run_slurm_script(
    ssh, indicators, models, scenarios, working_directory, input_dir
):
    """
    Task to create and run a Slurm script to run the indicator calculation scripts.

    Parameters:
    - ssh: Paramiko SSHClient object
    - indicators: Space-separated list of indicators to calculate
    - models: Space-separated list of models to calculate indicators for
    - scenarios: Space-separated list of scenarios to calculate indicators for
    - working_directory: Directory to where all of the processing takes place
    - input_dir: Directory containing the input data for the indicators
    """

    slurm_script = f"{working_directory}/cmip6-utils/indicators/slurm.py"

    stdin, stdout, stderr = ssh.exec_command(
        f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin:$HOME/miniconda3/bin && python {slurm_script} --indicators '{indicators}' --models '{models}' --scenarios '{scenarios}' --input_dir '{input_dir}' --working_dir '{working_directory}'"
    )

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    # Check the exit status for errors
    if exit_status != 0:
        error_output = stderr.read().decode("utf-8")
        raise Exception(
            f"Error creating or running Slurm scripts. Error: {error_output}"
        )

    print("Slurm scripts created and run successfully")


@task
def get_job_ids(ssh, username):
    """
    Task to get a list of job IDs for a specified user from the Slurm queue via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - username: Username to get job IDs for
    """

    stdin, stdout, stderr = ssh.exec_command(
        f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin && squeue -u {username} --format='%.18i %.9P %.50j %.8u %.8T %.10M %.9l %.6D %R'"
    )

    # Get a list of job IDs for the specified user
    job_ids = list()  # Skip header

    for line in stdout.readlines()[1:]:  # Skip header
        fields = line.split()
        job_id = fields[0]
        job_name = " ".join(fields[2])

        # Only add job IDs for indicator calculations
        if "indicator" in job_name:
            job_ids.append(job_id)

    # Prints the list of job IDs to the log for debugging purposes
    print(
        f"Job IDs for jobs started by {username} and containing 'indicator': {', '.join(job_ids)}"
    )
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
            stdin, stdout, stderr = ssh.exec_command(
                f"export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin && squeue -h -j {job_id}"
            )

            # If the job is no longer in the queue, remove it from the list
            if not stdout.read():
                job_ids.remove(job_id)

        if job_ids:
            # Sleep for a while before checking again
            sleep(10)

    print("All indicator jobs completed!")


@task
def qc(ssh, working_directory, input_dir):
    """
    Task to run the quality control (QC) script to check the output of the indicator calculations.

    Parameters:
    - ssh: Paramiko SSHClient object
    - working_directory: Directory to where all of the processing takes place
    """

    conda_init_script = f"{working_directory}/cmip6-utils/indicators/conda_init.sh"

    qc_script = f"{working_directory}/cmip6-utils/indicators/qc.py"
    output_dir = f"{working_directory}/output/"

    stdin, stdout, stderr = ssh.exec_command(
        f"source {conda_init_script}\n"
        f"conda activate cmip6-utils\n"
        f"python {qc_script} --out_dir '{output_dir}' --in_dir '{input_dir}'"
    )

    # Collect output from QC script above and print it
    lines = stdout.readlines()
    for line in lines:
        print(line)

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    # Check the exit status for errors
    if exit_status != 0:
        error_output = stderr.read().decode("utf-8")
        raise Exception(f"Error running QC script. Error: {error_output}")

    print("QC script run successfully")
