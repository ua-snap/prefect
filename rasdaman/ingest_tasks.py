import os
import tarfile
import shutil
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
    extraction_dir = os.path.join(data_directory, os.path.splitext(zip_file)[0])

    with zipfile.ZipFile(f"{data_directory}/{zip_file}", "r") as zip_ref:
        zip_file_names = zip_ref.namelist()

    all_files_exist = True
    for file_name in zip_file_names:
        if not os.path.exists(os.path.join(extraction_dir, file_name)):
            all_files_exist = False
            break

    if all_files_exist:
        print(
            f"All files from {zip_file} already exist in {extraction_dir}. Skipping extraction."
        )
        return

    if not os.path.exists(extraction_dir):
        os.makedirs(extraction_dir)

    with zipfile.ZipFile(f"{data_directory}/{zip_file}", "r") as zip_ref:
        zip_ref.extractall(extraction_dir)

    print(f"Unzipped {zip_file} to {extraction_dir}")


@task(name="Untar File")
def untar_file(tar_file, data_directory, flatten=False, rename=None):
    """
    Extract a .tar.gz file to a specified directory.

    Parameters
    ----------
    tar_file : str
        Path to the .tar.gz file.
    data_directory : str
        Path to the output directory.
    flatten : bool, optional
        If True, ignore directory structure in the archive and extract all files directly
        into a single directory. Default is False.
    rename : str, optional
        If provided, rename the extracted directory to this name after extraction.
        If target exists as a directory, it will be deleted first.
    """
    extraction_dir = os.path.join(
        data_directory, os.path.splitext(os.path.basename(tar_file))[0]
    )

    if flatten:
        temp_extract_dir = extraction_dir + "_tmp"
    else:
        temp_extract_dir = extraction_dir

    with tarfile.open(tar_file, "r:gz") as tar:
        tar_file_names = tar.getnames()

    all_files_exist = True
    for file_name in tar_file_names:
        target_name = os.path.basename(file_name) if flatten else file_name
        target_path = os.path.join(temp_extract_dir, target_name)
        if not os.path.exists(target_path):
            all_files_exist = False
            break

    if all_files_exist:
        print(f"All files from {tar_file} already exist in {temp_extract_dir}. Skipping extraction.")
        return

    if not os.path.exists(temp_extract_dir):
        os.makedirs(temp_extract_dir)

    with tarfile.open(tar_file, "r:gz") as tar:
        if flatten:
            for member in tar.getmembers():
                if member.isfile():
                    file_obj = tar.extractfile(member)
                    if file_obj is not None:
                        out_path = os.path.join(temp_extract_dir, os.path.basename(member.name))
                        with open(out_path, "wb") as out_file:
                            out_file.write(file_obj.read())
        else:
            tar.extractall(temp_extract_dir)

    # If rename is requested
    if flatten and rename:
        final_path = os.path.join(data_directory, rename)
        if os.path.exists(final_path):
            shutil.rmtree(final_path)
        os.rename(temp_extract_dir, final_path)
        print(f"Extracted {tar_file} to {final_path} (flatten={flatten}, renamed)")

    else:
        print(f"Extracted {tar_file} to {temp_extract_dir} (flatten={flatten})")

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
def run_python_script(python_script, ingest_directory, data_directory):
    # Run the merge script
    result = subprocess.run(
        ["python", python_script, "-d", data_directory],
        cwd=ingest_directory,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Error running the Python script. Error: {result.stderr}")

    print("Python Script Output:")
    print(result.stdout)


@task(name="Delete Coverage")
def delete_coverage(coverage_id):
    # Delete the coverage
    result = subprocess.run(
        ["/usr/local/bin/delete_coverage.sh", coverage_id],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Error deleting the coverage. Error: {result.stderr}")

    print("Coverage Deletion Output:")
    print(result.stdout)


@task(name="Run Rasdaman Ingest Script")
def run_ingest(ingest_directory, ingest_file="ingest.json", conda_env=False):
    command = [
        "/usr/local/bin/add_coverage.sh",
        ingest_file,
    ]

    if conda_env:
        command = [
            "bash",
            "-i",
            "-c",
            f"source /opt/miniconda3/bin/activate {conda_env} && {' '.join(command)}",
        ]
    env = {"LUTS_PATH": f"{ingest_directory}/luts.py"}
    result = subprocess.run(
        command, cwd=ingest_directory, env=env, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Error running the command. Error: {result.stderr}")

    print("Command Output:")
    print(result.stdout)
