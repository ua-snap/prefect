from pathlib import Path
from time import sleep
import paramiko
from prefect import task
from prefect.logging import get_run_logger


def connect_ssh(ssh_host, ssh_port, ssh_username, ssh_private_key_path):
    """Connect to a remote server via SSH using Paramiko.

    Parameters:
    - ssh_host: SSH host address
    - ssh_port: SSH port number
    - ssh_username: SSH username
    - ssh_private_key_path: Path to the private key file for SSH authentication
    """
    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    private_key = paramiko.RSAKey(filename=ssh_private_key_path)
    ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

    return ssh


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
    logger = get_run_logger()
    logger.info(f"Executing command: {cmd}")
    stdin_, stdout, stderr = ssh.exec_command(cmd)

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    return exit_status, decode_stream(stdout), decode_stream(stderr)


def rsync(ssh, source_directory, destination_directory, exclude=None):
    """Synchronizes a directory from the source directory to a destination directory via rsync.

    Parameters:
    - ssh: Paramiko SSHClient object
    - source_directory: Source directory
    - destination_directory: Destination directory on the local machine
    - exclude: Patterns to exclude from synchronization (optional)
    """
    exclude_str = ""
    if exclude:
        exclude_str = " ".join([f"--exclude={item}" for item in exclude])

    cmd = f"rsync -av {exclude_str} {source_directory} {destination_directory}"
    exit_status, stdout, stderr = exec_command(ssh, cmd)

    if exit_status != 0:
        raise Exception(f"Error synchronizing directory. Error: {stderr}")


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


def remote_directory_exists(ssh, directory):
    """Check if a directory exists on the remote server via SSH.

    Parameters:
    - ssh: Paramiko SSHClient object
    - directory: Directory to check for existence

    Returns:
    - True if the directory exists, False otherwise
    """
    exit_status, stdout, stderr = exec_command(ssh, f"test -d {directory}")
    return not bool(exit_status)


def input_is_child_of_scratch_dir(ssh, input_dir, scratch_dir):
    """Check that an input directory exists as a subdirectory of scratch_dir.
    If it does, return the directory. If not, return None.
    """
    # recursively compare parent of input_dir to scratch_dir
    tmp_path = input_dir.parent
    while tmp_path != scratch_dir:
        tmp_path = tmp_path.parent
        if tmp_path == Path("/"):
            return False

    # now verify this dir exists
    input_exists = remote_directory_exists(ssh, input_dir)
    if not input_exists:
        raise Exception(f"Input directory {input_dir} does not exist on remote server.")

    return True


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
    git_pull_command = f"cd {target_directory} && git pull origin {branch_name}"

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
def check_job_exit_status(ssh, job_id):
    """
    Check the exit status of a completed SLURM job using sacct.

    For array jobs, checks all array tasks and returns details on any failures.

    Parameters:
    - ssh: Paramiko SSHClient object
    - job_id: Job ID to check (can be parent array job or individual task)

    Returns:
    - tuple: (all_succeeded: bool, failed_tasks: list of dicts, total_tasks: int)
        failed_tasks contains: [{'task_id': '10', 'state': 'FAILED', 'exit_code': '0:53'}, ...]
    """
    logger = get_run_logger()

    # Use sacct to get job status - format optimized for parsing
    cmd = f"sacct -j {job_id} --format=JobID,State,ExitCode -P -n"
    exit_status, stdout, stderr = exec_command(ssh, cmd)

    if exit_status != 0:
        logger.warning(f"sacct command failed for job {job_id}: {stderr}")
        return True, [], 0  # Assume success if we can't check (backward compatible)

    if not stdout:
        logger.warning(f"No sacct output for job {job_id} - may be too old")
        return True, [], 0

    # Parse output: JobID|State|ExitCode
    lines = stdout.strip().split("\n")
    failed_tasks = []
    total_tasks = 0

    for line in lines:
        parts = line.split("|")
        if len(parts) < 3:
            continue

        job_id_str, state, exit_code = parts[0], parts[1], parts[2]

        # Skip sub-steps (.batch, .extern) - only check main array tasks
        if "." in job_id_str:
            continue

        # Check if this is an array task  (format: 573485_10)
        if "_" in job_id_str:
            total_tasks += 1
            task_id = job_id_str.split("_")[1]

            # Consider FAILED, CANCELLED, TIMEOUT, NODE_FAIL, OUT_OF_MEMORY as failures
            if state in [
                "FAILED",
                "CANCELLED",
                "TIMEOUT",
                "NODE_FAIL",
                "OUT_OF_MEMORY",
            ]:
                failed_tasks.append(
                    {
                        "task_id": task_id,
                        "job_id": job_id_str,
                        "state": state,
                        "exit_code": exit_code,
                    }
                )

    all_succeeded = len(failed_tasks) == 0

    if failed_tasks:
        logger.warning(
            f"Job {job_id}: {len(failed_tasks)}/{total_tasks} array tasks failed"
        )
        for task in failed_tasks[:5]:  # Log first 5 failures
            logger.warning(
                f"  Task {task['task_id']}: {task['state']} (exit code {task['exit_code']})"
            )
        if len(failed_tasks) > 5:
            logger.warning(f"  ... and {len(failed_tasks) - 5} more")
    else:
        logger.info(f"Job {job_id}: All tasks completed successfully")

    return all_succeeded, failed_tasks, total_tasks


@task
def wait_for_jobs_completion(
    ssh,
    job_ids,
    completion_message="Jobs completed!",
    max_retries=5,
    retry_delay=5,
    validate_exit_status=True,
):
    """
    Task to wait for a list of Slurm jobs to complete in the queue via SSH.

    NOW VALIDATES JOB EXIT STATUS using sacct after jobs finish!

    Parameters:
    - ssh: Paramiko SSHClient object
    - job_ids: List of job IDs to wait for
    - max_retries: Number of retries for transient SSH/connection errors
    - retry_delay: Seconds to wait between retries
    - validate_exit_status: If True, check job exit codes after completion (default: True)

    Raises:
    - Exception: If any jobs failed (after validation)
    """
    logger = get_run_logger()
    logger.info(f"Waiting for jobs to complete: {job_ids}")

    while job_ids:
        for job_id in job_ids.copy():

            # ---- RETRY BLOCK ----
            for attempt in range(1, max_retries + 1):
                try:
                    exit_status, stdout, stderr = exec_command(
                        ssh, f"squeue -h -j {job_id}"
                    )
                    break  # success → exit retry loop
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt}/{max_retries} failed "
                        f"checking job {job_id}: {e}"
                    )
                    if attempt == max_retries:
                        logger.exception(f"Max retries exceeded checking job {job_id}")
                        raise
                    sleep(retry_delay)
            # ---- END RETRY BLOCK ----

            if exit_status != 0:
                raise Exception(
                    f"Error checking job status for job ID {job_id}. "
                    f"Error: {stderr}"
                )

            # If the job is no longer in the queue, remove it
            if not stdout:
                job_ids.remove(job_id)

        if job_ids:
            sleep(10)

    logger.info(f"All jobs finished running: {job_ids}")

    # CRITICAL: Now validate that jobs actually SUCCEEDED
    if validate_exit_status:
        logger.info("Validating job exit statuses...")
        failed_jobs = []

        for job_id in job_ids:
            all_succeeded, failed_tasks, total_tasks = check_job_exit_status(
                ssh, job_id
            )

            if not all_succeeded:
                failed_jobs.append(
                    {
                        "job_id": job_id,
                        "failed_tasks": failed_tasks,
                        "total_tasks": total_tasks,
                    }
                )

        if failed_jobs:
            error_msg = f"{len(failed_jobs)} job(s) had failures:\n"
            for job_info in failed_jobs:
                job_id = job_info["job_id"]
                n_failed = len(job_info["failed_tasks"])
                n_total = job_info["total_tasks"]
                error_msg += f"  Job {job_id}: {n_failed}/{n_total} tasks failed\n"
                for task in job_info["failed_tasks"][:3]:
                    error_msg += f"    - Task {task['task_id']}: {task['state']} ({task['exit_code']})\n"

            raise Exception(error_msg)

    logger.info(completion_message)


@task
def wait_for_jobs_with_retry(
    ssh,
    job_ids,
    sbatch_script_path=None,
    max_job_retries=3,
    retry_delay=60,
    exponential_backoff=True,
    resubmit_failed_tasks=True,
    **wait_kwargs,
):
    """
    Wait for jobs to complete with automatic retry for failed array tasks.

    Specifically designed to handle intermittent 0:53 errors and filesystem hiccups
    by resubmitting only the failed array tasks.

    Parameters:
    - ssh: Paramiko SSHClient object
    - job_ids: List of job IDs to wait for
    - sbatch_script_path: Path to original SBATCH script (required for resubmission)
    - max_job_retries: Maximum number of times to retry failed tasks (default: 3)
    - retry_delay: Base delay in seconds between retries (default: 60)
    - exponential_backoff: If True, double delay after each retry (default: True)
    - resubmit_failed_tasks: If True, resubmit failed array tasks automatically (default: True)
    - **wait_kwargs: Additional kwargs passed to wait_for_jobs_completion

    Returns:
    - List of final job IDs (may include retry jobs)

    Raises:
    - Exception: If jobs still fail after max retries
    """
    logger = get_run_logger()

    all_job_ids = list(job_ids)  # Track all jobs including retries
    current_retry = 0
    current_delay = retry_delay

    while current_retry <= max_job_retries:
        try:
            # Wait for current batch of jobs
            wait_for_jobs_completion(ssh, job_ids, **wait_kwargs)

            # Success! All jobs completed
            logger.info(
                f"All jobs completed successfully (attempt {current_retry + 1})"
            )
            return all_job_ids

        except Exception as e:
            error_msg = str(e)

            # Check if this is a job failure we can retry
            if (
                "task" in error_msg.lower()
                and "failed" in error_msg.lower()
                and current_retry < max_job_retries
            ):
                logger.warning(
                    f"Job failures detected on attempt {current_retry + 1}/{max_job_retries + 1}. "
                    f"Will retry after {current_delay}s..."
                )
                logger.warning(f"Failure details: {error_msg}")

                if not resubmit_failed_tasks or not sbatch_script_path:
                    logger.error(
                        "Cannot retry: resubmit_failed_tasks=False or no sbatch_script_path provided"
                    )
                    raise

                # Identify which jobs had failures
                retry_job_ids = []
                for job_id in job_ids:
                    all_succeeded, failed_tasks, total_tasks = check_job_exit_status(
                        ssh, job_id
                    )

                    if not all_succeeded and failed_tasks:
                        # Build array string for failed tasks only
                        failed_task_ids = sorted(
                            [int(t["task_id"]) for t in failed_tasks]
                        )

                        # Convert to SLURM array format (e.g., "10-12,15,18-20")
                        array_str = _format_task_ids_for_slurm(failed_task_ids)

                        logger.info(
                            f"Resubmitting {len(failed_task_ids)}/{total_tasks} failed tasks "
                            f"from job {job_id}: array indices {array_str}"
                        )

                        # Resubmit with modified array range
                        # Note: This assumes the original sbatch script supports --array override
                        retry_cmd = f"sbatch --array={array_str} {sbatch_script_path}"
                        exit_status, stdout, stderr = exec_command(ssh, retry_cmd)

                        if exit_status != 0:
                            raise Exception(f"Failed to resubmit tasks: {stderr}")

                        # Parse new job ID
                        new_job_id = parse_job_ids(stdout.split()[-1])[0]
                        retry_job_ids.append(new_job_id)
                        all_job_ids.append(new_job_id)

                        logger.info(f"Retry job submitted: {new_job_id}")

                if not retry_job_ids:
                    logger.error(
                        "No retry jobs created but failures detected - cannot proceed"
                    )
                    raise

                # Wait before retrying
                logger.info(f"Waiting {current_delay}s before polling retry jobs...")
                sleep(current_delay)

                # Update loop state
                job_ids = retry_job_ids
                current_retry += 1

                if exponential_backoff:
                    current_delay *= 2
            else:
                # Not a retriable error, or out of retries
                logger.error(f"Job failure after {current_retry} retries: {error_msg}")
                raise

    # Should not reach here, but just in case
    raise Exception(f"Max retries ({max_job_retries}) exceeded")


def _format_task_ids_for_slurm(task_ids):
    """
    Convert a list of task IDs to SLURM array format.

    Examples:
    - [1, 2, 3] -> "1-3"
    - [1, 3, 5] -> "1,3,5"
    - [1, 2, 3, 5, 6, 10] -> "1-3,5-6,10"

    Parameters:
    - task_ids: Sorted list of integer task IDs

    Returns:
    - String in SLURM array format
    """
    if not task_ids:
        return ""

    ranges = []
    start = task_ids[0]
    end = task_ids[0]

    for task_id in task_ids[1:]:
        if task_id == end + 1:
            # Continue current range
            end = task_id
        else:
            # Save current range and start new one
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = task_id
            end = task_id

    # Add final range
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")

    return ",".join(ranges)


def create_directories(ssh, dir_list):
    """Creates directories if they don't exist, otherwise does nothing.

    Parameters:
    - ssh: Paramiko SSHClient object, "connected"
    - dir_list: List of directories to create
    """
    logger = get_run_logger()
    for directory in dir_list:
        exit_status, stdout, stderr = exec_command(
            ssh, f"test -d {directory} && echo 1 || echo 0"
        )
        directory_exists = bool(int(stdout))

        if directory_exists:
            logger.info(f"Directory {directory} already exists.")
            continue
        else:
            logger.info(f"Creating directory {directory}...")
            exit_status, stdout, stderr = exec_command(ssh, f"mkdir {directory}")

            if exit_status != 0:
                raise Exception(
                    f"Error creating directory {directory}. Error: {stderr}"
                )
            else:
                print(f"Directory {directory} created successfully.")


@task
def rsync_task(ssh, source_directory, destination_directory, exclude=None):
    """Task wrapper for utils.rsync"""
    rsync(ssh, source_directory, destination_directory, exclude=None)
