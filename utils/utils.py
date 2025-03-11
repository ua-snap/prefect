from pathlib import Path
from time import sleep
from prefect import task
import logging


def decode_stream(std):
    """Decode an stderr or stdout stream from a Paramiko SSHClient object.

    Parameters:
    - std: Paramiko stdout or stderr stream
    """
    return std.read().decode("utf-8").strip()


def exec_command(ssh, cmd):
    """Execute a command on a remote server via SSH and return the output and exit status.

    Parameters:
    - ssh: Paramiko SSHClient object
    - cmd: Command to execute on the remote server
    """
    stdin_, stdout, stderr = ssh.exec_command(cmd)

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    return exit_status, decode_stream(stdout), decode_stream(stderr)


def parse_job_ids(stdout):
    """Parse the job IDs from stdout.
    Any time we are expecting job IDs from stdout, it should be a string of " "-separated IDs.
    Use this function to ensure we are getting job IDs in the correct format.

    Parameters:
    - stdout: stdout from a command that returns job IDs
    """
    job_ids = stdout.split(" ")

    try:
        # Check if the job IDs are integers
        job_ids = [int(job_id) for job_id in job_ids]
    except ValueError:
        raise ValueError(
            f"Non-integer job IDs found in output. stdout parsed: {stdout}, job_ids: {job_ids}"
        )

    return job_ids


@task
def check_for_nfs_mount(ssh, nfs_directory="/import/beegfs"):
    """
    Task to check if an NFS directory is mounted on the remote server via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - nfs_directory: Path to the NFS directory to check for
    """

    stdin_, stdout, stderr_ = ssh.exec_command(f"df -h | grep {nfs_directory}")

    nfs_mounted = bool(stdout.read())

    if not nfs_mounted:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task
def clone_github_repository(ssh, repo_name, branch_name, destination_directory):
    """
    Task to clone a GitHub repository via SSH and switch to a specific branch if it exists.

    Parameters:
    - ssh: Paramiko SSHClient object
    - repo_name: Name of the repository to clone
    - branch_name: Name of the branch to clone and switch to
    - destination_directory: Directory to clone the repository into
    """

    target_directory = Path(f"{destination_directory}/{repo_name}")
    exit_status, stdout, stderr = exec_command(
        ssh, f"if [ -d '{target_directory}' ]; then echo 'true'; else echo 'false'; fi"
    )

    clone_error_exception_msg = (
        "Error cloning the GitHub repository. Exit status: {exit_status}. "
        "Error: {error_msg}. Original command: {cmd}"
    )

    directory_exists = stdout == "true"

    if directory_exists:
        print(f"Repository exists at {target_directory}.")

        try:
            # Directory exists, check the current branch
            get_current_branch_command = (
                f"cd {target_directory} && git pull && git branch --show-current"
            )
            exit_status, stdout, stderr = exec_command(ssh, get_current_branch_command)
            current_branch = stdout
        except:
            # If the current branch cannot be determined, assume it's the wrong branch
            set_branch_to_main = f"cd {target_directory} && git checkout main"
            exit_status, stdout, stderr = exec_command(ssh, set_branch_to_main)

            # Get the current branch again# Directory exists, check the current branch
            get_current_branch_command = (
                f"cd {target_directory} && git pull && git branch --show-current"
            )
            exit_status, stdout, stderr = exec_command(ssh, get_current_branch_command)
            if exit_status != 0:
                raise Exception(
                    f"Error executing {get_current_branch_command}. Error: {stderr}"
                )
            current_branch = stdout

        if current_branch != branch_name:
            print(f"Change repository branch to branch {branch_name}...")
            # If the current branch is different from the desired branch, switch to the correct branch
            switch_branch_command = (
                f"cd {target_directory} && git checkout {branch_name}"
            )
            exit_status, stdout, stderr = exec_command(ssh, switch_branch_command)
            if exit_status != 0:
                raise Exception(
                    f"Error switching to branch {branch_name}. Error: {stderr}"
                )

        print(f"Pulling the GitHub repository on branch {branch_name}...")

        # Run the Git pull command to pull the repository
        git_pull_command = f"cd {target_directory} && git pull origin {branch_name}"
        exit_status, stdout, stderr = exec_command(ssh, git_pull_command)

        # Check the exit status for errors
        if exit_status != 0:
            raise Exception(
                clone_error_exception_msg.format(
                    exit_status=exit_status,
                    error_msg=stderr,
                    cmd=git_pull_command,
                )
            )
    else:
        print(
            f"No existing repo found at {target_directory}. Cloning repository from github.com/ua-snap on branch {branch_name}..."
        )
        # Run the Git clone command to clone the repository
        git_command = f"cd {destination_directory} && git clone -b {branch_name} https://github.com/ua-snap/{repo_name}.git"
        exit_status, stdout, stderr = exec_command(ssh, git_command)

        # Check the exit status for errors
        if exit_status != 0:
            raise Exception(
                clone_error_exception_msg.format(
                    exit_status=exit_status,
                    error_msg=stderr,
                    cmd=git_pull_command,
                )
            )

    return target_directory


@task
def install_conda(ssh):
    """Check for a conda installation on the remote server via SSH and install Miniconda if it doesn't exist.

    Parameters:
    - ssh: Paramiko SSHClient object
    """
    # doing this to make sure that conda is not installed, not just checking for a miniconda3 folder
    exit_status, stdout, stderr = exec_command(
        ssh, "test $HOME/miniconda3 && echo 1 || echo 0"
    )
    conda_installed = bool(int(stdout))
    assert (
        not conda_installed
    ), f"install_conda called, but conda installation found at {stdout}."

    conda_uri = "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
    print("No conda installation found. Installing Miniconda ...")
    # Download and install Miniconda
    install_miniconda_cmd = (
        f"wget {conda_uri} -O miniconda.sh && bash miniconda.sh -b -p $HOME/miniconda3"
    )
    exit_status, stdout, stderr = exec_command(ssh, install_miniconda_cmd)

    if exit_status != 0:
        error_output = stderr
        raise Exception(f"Error installing Miniconda. Error: {error_output}")

    print("Miniconda installed successfully")


@task
def initialize_shell_for_conda(ssh):
    """Initialize the shell for conda usage.

    Parameters:
    - ssh: Paramiko SSHClient object
    """
    exit_status, stdout, stderr = exec_command(
        ssh, 'eval "$($HOME/miniconda3/bin/conda shell.bash hook)"'
    )

    if exit_status != 0:
        error_output = stderr
        raise Exception(f"Error initializing shell for Conda. Error: {error_output}")


@task
def ensure_conda(ssh):
    """Check for a conda installation on the remote server via SSH.
    Install and initalize (i.e. make conda executable available on PATH, set some env vars etc) shell if not installed.
    If installed ($HOME/miniconda folder found) and not initialized, initialize shell for conda usage.

    Parameters:
    - ssh: Paramiko SSHClient object
    """
    exit_status, stdout, stderr = exec_command(ssh, "which conda")
    # if this works, then conda is installed and shell is initialized
    # but if it fails, conda might be installed but with unitialized shell
    shell_initialized = bool(stdout)

    if not shell_initialized:
        # check for miniconda3 folder as proxy for conda is installed
        exit_status, stdout, stderr = exec_command(
            ssh, "test -d $HOME/miniconda3 && echo 1 || echo 0"
        )
        miniconda_installed = bool(stdout)

        if not miniconda_installed:
            print("Conda not installed.")
            install_conda(ssh)

        print("Shell not initialized for Conda. Initializing...")
        initialize_shell_for_conda(ssh)


@task
def ensure_slurm(ssh):
    """Check for SLURM tools on the remote server via SSH, and load the slurm module if not found.

    Parameters:
    - ssh: Paramiko SSHClient object
    """
    # Check for the SLURM sbatch command, use as proxy for all tools
    exit_status, stdout, stderr = exec_command(ssh, "which sbatch")
    slurm_tools_installed = bool(stdout)

    if not slurm_tools_installed:
        exit_status, stdout, stderr = exec_command(ssh, "module load slurm")
        if exit_status != 0:
            raise Exception("Unable to load slurm module on remote server.")


@task
def create_conda_environment(ssh, conda_env_name, conda_env_file):
    """
    Task to create a conda environment from and environment file spec.

    Parameters:
    - ssh: Paramiko SSHClient object
    - conda_env_name: Name of the Conda environment to create/install
    - conda_env_file: Path to the Conda environment file (.yml) to use for installation
    """
    # Install the Conda environment from the environment file
    install_cmd = f"conda env create -n {conda_env_name} -f {conda_env_file}"
    exit_status, stdout, stderr = exec_command(ssh, install_cmd)

    if exit_status == 0:
        print(f"Conda environment '{conda_env_name}' created successfully.")
    else:
        raise Exception(
            f"Error creating Conda environment '{conda_env_name}'. Error: {stderr}"
        )


@task
def ensure_conda_env(ssh, conda_env_name, conda_env_file):
    """
    Task to check for a conda environment on the user's account via SSH and create it if not found.

    Parameters:
    - ssh: Paramiko SSHClient object
    - conda_env_name: Name of the Conda environment to check for
    - conda_env_file: Path to the Conda environment file (.yml) to use for installation
    """
    # Check if the Conda environment already exists
    exit_status, stdout, stderr = exec_command(
        ssh, f"conda env list | grep {conda_env_name}"
    )
    conda_env_exists = bool(stdout)

    if conda_env_exists:
        print(f"Conda environment '{conda_env_name}' already exists.")
    else:
        print(f"Conda environment '{conda_env_name}' not found. Creating...")
        create_conda_environment(ssh, conda_env_name, conda_env_file)


@task
def get_job_ids(ssh, username):
    """
    Task to get a list of job IDs for a specified user from the Slurm queue via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - username: Username to get job IDs for
    """
    # skip headers, print only the job IDs
    exit_status, stdout, stderr = exec_command(
        ssh, f"squeue -u {username} --noheader -o %i "
    )

    # Get a list of job IDs for the specified user
    job_ids = stdout.split("\n")

    # Prints the list of job IDs to the log for debugging purposes
    print(job_ids)
    return job_ids


@task
def wait_for_jobs_completion(ssh, job_ids, completion_message="Jobs completed!"):
    """
    Task to wait for a list of Slurm jobs to complete in the queue via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - job_ids: List of job IDs to wait for
    """

    while job_ids:
        # Check the status of each job in the list
        for job_id in job_ids.copy():
            exit_status, stdout, stderr = exec_command(ssh, f"squeue -h -j {job_id}")

            # If the job is no longer in the queue, remove it from the list
            if not stdout:
                job_ids.remove(job_id)

        if job_ids:
            # Sleep for a while before checking again
            sleep(10)

    logging.info(completion_message)


@task
def create_directories(ssh, dir_list):
    """Creates directories if they don't exist, otherwise does nothing.

    Parameters:
    - ssh: Paramiko SSHClient object
    - dir_list: List of directories to create
    """
    for directory in dir_list:
        exit_status, stdout, stderr = exec_command(
            ssh, f"test -d {directory} && echo 1 || echo 0"
        )
        directory_exists = bool(int(stdout))

        if directory_exists:
            logging.info(f"Directory {directory} already exists.")
            continue
        else:
            logging.info(f"Creating directory {directory}...")
            exit_status, stdout, stderr = exec_command(ssh, f"mkdir {directory}")

            if exit_status != 0:
                raise Exception(
                    f"Error creating directory {directory}. Error: {stderr}"
                )
            else:
                print(f"Directory {directory} created successfully.")
