from prefect import task
import paramiko

# Define your SSH parameters
ssh_host = "datacubes.earthmaps.io"
ssh_port = 22


@task
def check_for_nfs_mount(ssh, nfs_directory="/CKAN_Data"):
    stdin, stdout, stderr = ssh.exec_command(f"df -h | grep {nfs_directory}")

    nfs_mounted = bool(stdout.read())

    if not nfs_mounted:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task
def copy_data_from_nfs_mount(ssh, source_directory, destination_directory):
    stdin, stdout, stderr = ssh.exec_command(
        f"rsync -av --usermap='*:$(id -u -n)' {source_directory} {destination_directory}"
    )
    exit_status = stdout.channel.recv_exit_status()

    # Check the exit status for errors
    if exit_status != 0:
        error_output = stderr.read().decode("utf-8")
        raise Exception(
            f"Failed to copy data from NFS mount at {source_directory}. Error: {error_output}"
        )


@task
def unzip_files(ssh, data_directory, unzipped_directory=None):
    # Check if only .zip and .png files exist in the target directory
    check_files_exist_command = (
        f"cd {data_directory} && ls | grep -v -E '(\.zip$|\.png$)' | wc -l"
    )
    stdin, stdout, stderr = ssh.exec_command(check_files_exist_command)
    file_count = int(stdout.read().strip())

    if file_count == 0:
        stdin, stdout, stderr = ssh.exec_command(
            f"cd {data_directory} && unzip '*.zip'"
        )
        exit_status = stdout.channel.recv_exit_status()

        # Check the exit status for errors
        if exit_status != 0:
            error_output = stderr.read().decode("utf-8")
            raise Exception(
                f"Failed to unzip files in {data_directory}. Error: {error_output}"
            )
        if unzipped_directory:
            # Move the unzipped files to the unzipped directory
            stdin, stdout, stderr = ssh.exec_command(
                f"mv {unzipped_directory}/* {data_directory}"
            )
            exit_status = stdout.channel.recv_exit_status()

            # Check the exit status for errors
            if exit_status != 0:
                error_output = stderr.read().decode("utf-8")
                raise Exception(
                    f"Failed to move unzipped files to {unzipped_directory}. Error: {error_output}"
                )


@task
def clone_github_repository(ssh, branch, destination_directory):
    target_directory = f"{destination_directory}/rasdaman-ingest"
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
        git_command = f"cd {destination_directory} && git clone -b {branch} https://github.com/ua-snap/rasdaman-ingest"
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
def run_ingest(ssh, ingest_directory, ingest_file="ingest.json"):
    # Run the command
    stdin, stdout, stderr = ssh.exec_command(
        f"cd {ingest_directory} && export LUTS_PATH={ingest_directory}luts.py && /opt/rasdaman/bin/wcst_import.sh -c 0 {ingest_file}"
    )

    # Wait for the command to finish and get the exit status
    exit_status = stdout.channel.recv_exit_status()

    # Check the exit status for errors
    if exit_status != 0:
        error_output = stderr.read().decode("utf-8")
        raise Exception(f"Error running the command. Error: {error_output}")

    # Capture the standard output (stdout) and standard error (stderr)
    command_output = stdout.read().decode("utf-8")

    # Print the command output
    print("Command Output:")
    print(command_output)
