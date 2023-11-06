from prefect import flow, task
import paramiko

# Define your SSH parameters
ssh_host = "datacubes.earthmaps.io"
ssh_port = 22


@task
def check_for_nfs_mount(ssh):
    stdin, stdout, stderr = ssh.exec_command("df -h | grep /Data")

    nfs_mounted = bool(stdout.read())

    if not nfs_mounted:
        raise Exception(f"NFS directory '/Data' is not mounted")


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
def unzip_files(ssh, data_directory):
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
def run_ingest(ssh, ingest_directory):
    # Run the command
    stdin, stdout, stderr = ssh.exec_command(
        f"cd {ingest_directory} && export LUTS_PATH={ingest_directory}luts.py && /opt/rasdaman/bin/wcst_import.sh -c 0 ingest.json"
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


@flow(log_prints=True)
def ingest_flow(
    ssh_username,
    ssh_private_key_path,
    branch_name,
    working_directory,
    ingest_directory,
    source_directory,
    destination_directory,
):
    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        clone_github_repository(ssh, branch_name, working_directory)

        check_for_nfs_mount(ssh)

        copy_data_from_nfs_mount(ssh, source_directory, destination_directory)

        unzip_files(ssh, destination_directory)

        run_ingest(ssh, ingest_directory)
    finally:
        ssh.close()


if __name__ == "__main__":
    ingest_flow.serve(
        name="gipl-ingest",
        tags=["crrel_gipl_outputs"],
        parameters={
            "ssh_username": "rltorgerson",
            "ssh_private_key_path": "/Users/rltorgerson/.ssh/id_rsa",
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/rltorgerson/",
            "ingest_directory": "/opt/rasdaman/rltorgerson/rasdaman-ingest/ardac/gipl/",
            "source_directory": "/Data/Base/AK_1km/GIPL/",
            "destination_directory": "/opt/rasdaman/rltorgerson/rasdaman-ingest/ardac/gipl/geotiffs/",
        },
    )
