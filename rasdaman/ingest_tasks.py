import os
import tarfile
from prefect import task
import subprocess
import zipfile


@task(name="Check for NFS Mount")
def check_for_nfs_mount(nfs_directory="/CKAN_Data"):
    # Check if NFS directory is mounted
    result = subprocess.run(
        ["df", "-h", "|", "grep", nfs_directory], capture_output=True, text=True
    )
    if not result.stdout:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task(name="Copy Data from NFS Mount")
def copy_data_from_nfs_mount(source_directory, destination_directory, only_files=False):
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)

    username = os.popen("id -u -n").read().strip()

    rsync_command = [
        "rsync",
        "-av",
        f"--usermap=*:{username}",
    ]

    if only_files:
        # Only copy files (no directory structure)
        rsync_command.extend(["--include='*/'", "--include='*.*'", "--exclude='*/'"])
    rsync_command.extend([source_directory, destination_directory])

    result = subprocess.run(
        rsync_command,
        capture_output=True,
        text=True,
        shell=False,
    )
    if result.returncode != 0:
        raise Exception(
            f"Failed to copy data from NFS mount at {source_directory}. Error: {result.stderr}"
        )


@task(name="Unzip Files")
def unzip_files(data_directory, zip_file):

    with zipfile.ZipFile(f"{data_directory}/{zip_file}", "r") as zip_ref:
        zip_ref.extractall(data_directory)


@task(name="Untar File")
def untar_file(tar_file, data_directory):
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    if not os.path.exists(tar_file):
        raise Exception(f"Tar file {tar_file} does not exist")

    with tarfile.open(tar_file, "r:gz") as tar:
        tar.extractall(data_directory)


@task(name="Clone GitHub Repository")
def clone_github_repository(branch, destination_directory):
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)

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


@task(name="Run Python Script")
def run_python_script(python_script, data_directory):
    # Run the merge script
    result = subprocess.run(
        ["python", python_script, "--directory", data_directory],
        cwd=data_directory,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Error running the Python script. Error: {result.stderr}")

    print("Python Script Output:")
    print(result.stdout)


@task(name="Run Rasdaman Ingest Script")
def run_ingest(ingest_directory, ingest_file="ingest.json", conda_env=False):
    if conda_env:
        command = [
            "conda",
            "run",
            "-n",
            conda_env,
            "/usr/local/bin/add_coverage.sh",
            ingest_file,
        ]
    else:
        command = [
            "/usr/local/bin/add_coverage.sh",
            ingest_file,
        ]
    env = {"LUTS_PATH": f"{ingest_directory}/luts.py"}
    result = subprocess.run(
        command, cwd=ingest_directory, env=env, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Error running the command. Error: {result.stderr}")

    print("Command Output:")
    print(result.stdout)
