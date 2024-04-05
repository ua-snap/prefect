import os
import tarfile
from prefect import task
import subprocess
import zipfile


@task
def check_for_nfs_mount(nfs_directory="/CKAN_Data"):
    # Check if NFS directory is mounted
    result = subprocess.run(
        ["df", "-h", "|", "grep", nfs_directory], capture_output=True, text=True
    )
    if not result.stdout:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task
def copy_data_from_nfs_mount(source_directory, destination_directory):
    # Copy data from NFS mount
    result = subprocess.run(
        [
            "rsync",
            "-av",
            "--usermap=*:$(id -u -n)",
            source_directory,
            destination_directory,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(
            f"Failed to copy data from NFS mount at {source_directory}. Error: {result.stderr}"
        )


@task
def unzip_files(data_directory, zip_file):

    with zipfile.ZipFile(f"{data_directory}/{zip_file}", "r") as zip_ref:
        zip_ref.extractall(data_directory)


@task
def untar_file(tar_file, data_directory):
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    if not os.path.exists(tar_file):
        raise Exception(f"Tar file {tar_file} does not exist")

    with tarfile.open(tar_file, "r:gz") as tar:
        tar.extractall(data_directory)


@task
def clone_github_repository(branch, destination_directory):
    # Clone or pull GitHub repository
    target_directory = f"{destination_directory}/rasdaman-ingest"
    result = subprocess.run(
        [f"if [ -d '{target_directory}' ]; then echo 'true'; else echo 'false'; fi"],
        shell=True,
        capture_output=True,
        text=True,
    )
    directory_exists = result.stdout.strip() == "true"

    if directory_exists:
        get_current_branch_command = ["git", "branch", "--show-current"]
        result = subprocess.run(
            get_current_branch_command,
            cwd=target_directory,
            capture_output=True,
            text=True,
        )
        current_branch = result.stdout.strip()

        if current_branch != branch:
            switch_branch_command = ["git", "checkout", branch]
            result = subprocess.run(
                switch_branch_command,
                cwd=target_directory,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise Exception(
                    f"Error switching to branch {branch}. Error: {result.stderr}"
                )

        git_pull_command = ["git", "pull", "origin", branch]
        result = subprocess.run(
            git_pull_command, cwd=target_directory, capture_output=True, text=True
        )
        if result.returncode != 0:
            raise Exception(
                f"Error pulling the GitHub repository. Error: {result.stderr}"
            )
    else:
        git_clone_command = [
            "git",
            "clone",
            "-b",
            branch,
            "https://github.com/ua-snap/rasdaman-ingest",
        ]
        result = subprocess.run(
            git_clone_command, cwd=destination_directory, capture_output=True, text=True
        )
        if result.returncode != 0:
            raise Exception(
                f"Error cloning the GitHub repository. Error: {result.stderr}"
            )


@task
def run_ingest(ingest_directory, ingest_file="ingest.json"):
    # Run the ingest command
    command = ["/opt/rasdaman/bin/wcst_import.sh", "-c", "0", ingest_file]
    env = {"LUTS_PATH": f"{ingest_directory}/luts.py"}
    result = subprocess.run(
        command, cwd=ingest_directory, env=env, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Error running the command. Error: {result.stderr}")

    print("Command Output:")
    print(result.stdout)
