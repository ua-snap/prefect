from time import sleep
from prefect import task
import paramiko


@task
def check_for_nfs_mount(ssh, nfs_directory="/import/beegfs"):
    stdin, stdout, stderr = ssh.exec_command(f"df -h | grep {nfs_directory}")

    nfs_mounted = bool(stdout.read())

    if not nfs_mounted:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task
def clone_github_repository(ssh, branch, destination_directory):
    target_directory = f"{destination_directory}/cmip6-utils"
    stdin, stdout, stderr = ssh.exec_command(
        f"if [ -d '{target_directory}' ]; then echo 'true'; else echo 'false'; fi"
    )

    directory_exists = stdout.read().decode("utf-8").strip() == "true"

    if directory_exists:
        # Directory exists, check the current branch
        get_current_branch_command = (
            f"cd {target_directory} && git branch --show-current"
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
def create_and_run_slurm_script(
    ssh, indicators, models, scenarios, slurm_script, input_dir, output_dir
):
    stdin, stdout, stderr = ssh.exec_command(
        f"source ~/.bashrc && export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin && python {slurm_script} --indicators '{indicators}' --models '{models}' --scenarios '{scenarios}' --input_dir '{input_dir}' --out_dir '{output_dir}'"
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
    stdin, stdout, stderr = ssh.exec_command(
        f"source ~/.bashrc && export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin && squeue -u {username}"
    )

    # Get a list of job IDs for the specified user
    job_ids = [line.split()[0] for line in stdout.readlines()[1:]]  # Skip header

    print(job_ids)
    return job_ids


@task
def wait_for_jobs_completion(ssh, job_ids):
    while job_ids:
        # Check the status of each job in the list
        for job_id in job_ids.copy():
            stdin, stdout, stderr = ssh.exec_command(
                f"source ~/.bashrc && export PATH=$PATH:/opt/slurm-22.05.4/bin:/opt/slurm-22.05.4/sbin && squeue -h -j {job_id}"
            )

            # If the job is no longer in the queue, remove it from the list
            if not stdout.read():
                job_ids.remove(job_id)

        if job_ids:
            # Sleep for a while before checking again
            sleep(10)

    print("All indicator jobs completed!")


@task
def qc(ssh, qc_script, output_dir):
    conda_init_script = (
        "/beegfs/CMIP6/jdpaul3/scratch/cmip6-utils/indicators/conda_init.sh"
    )

    stdin, stdout, stderr = ssh.exec_command(
        f"source {conda_init_script}\n"
        f"conda activate cmip6-utils\n"
        f"python {qc_script} --out_dir '{output_dir}'"
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
