from prefect import task
import subprocess


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
def unzip_files(data_directory, unzipped_directory=None):
    # Unzip files
    check_files_exist_command = [
        "ls",
        data_directory,
        "|",
        "grep",
        "-E",
        "(\.zip$|\.png$)",
        "|",
        "wc",
        "-l",
    ]
    result = subprocess.run(check_files_exist_command, capture_output=True, text=True)
    file_count = int(result.stdout.strip())

    if file_count == 0:
        unzip_command = ["unzip", f"{data_directory}/*.zip", "-d", data_directory]
        result = subprocess.run(unzip_command, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(
                f"Failed to unzip files in {data_directory}. Error: {result.stderr}"
            )

        if unzipped_directory:
            move_command = ["mv", f"{unzipped_directory}/*", data_directory]
            result = subprocess.run(move_command, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(
                    f"Failed to move unzipped files to {unzipped_directory}. Error: {result.stderr}"
                )


@task
def install_conda_environment(conda_env_name, conda_env_file, local_install=False):
    """
    Task to check for a Python Conda environment and install it if it doesn't exist.
    It also checks for Miniconda installation and installs Miniconda if it doesn't exist.
    """

    if local_install:
        # Check if the Miniconda directory exists
        miniconda_found = subprocess.run(
            "test -d $HOME/miniconda3 && echo 1 || echo 0",
            shell=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        miniconda_installed = bool(int(miniconda_found))

        if not miniconda_installed:
            print("Miniconda directory not found. Installing Miniconda...")
            # Download and install Miniconda
            subprocess.run(
                "wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && bash miniconda.sh -b -p $HOME/miniconda3",
                shell=True,
                check=True,
            )
            print("Miniconda installed successfully")

        # Check if the Conda environment already exists
        conda_env_exists = (
            subprocess.run(
                f"source $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env list | grep {conda_env_name}",
                shell=True,
            ).returncode
            == 0
        )

        if not conda_env_exists:
            print(f"Conda environment '{conda_env_name}' does not exist. Installing...")

            # Install the Conda environment from the environment file
            subprocess.run(
                f"source $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env create -n {conda_env_name} -f {conda_env_file}",
                shell=True,
                check=True,
            )
            print(f"Conda environment '{conda_env_name}' installed successfully")
        else:
            print(f"Conda environment '{conda_env_name}' already exists.")
    else:
        # Check if the Miniconda directory exists
        miniconda_found = subprocess.run(
            "test -d /opt/miniconda3 && echo 1 || echo 0",
            shell=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        miniconda_installed = bool(int(miniconda_found))

        if not miniconda_installed:
            print("/opt/miniconda3 directory not found.")
            exit(1)

        # Check if the Conda environment already exists
        conda_env_exists = (
            subprocess.run(
                f"source /opt/miniconda3/bin/activate && /opt/miniconda3/bin/conda env list | grep {conda_env_name}",
                shell=True,
            ).returncode
            == 0
        )

        if not conda_env_exists:
            print(f"Conda environment '{conda_env_name}' does not exist. Installing...")

            # Install the Conda environment from the environment file
            subprocess.run(
                f"source /opt/miniconda3/bin/activate && /opt/miniconda3/bin/conda env create -n {conda_env_name} -f {conda_env_file}",
                shell=True,
                check=True,
            )
            print(f"Conda environment '{conda_env_name}' installed successfully")
        else:
            print(f"Conda environment '{conda_env_name}' already exists.")


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


@task
def merge_data(data_directory):
    # Run the merge script
    merge_script = f"{data_directory}/merge.py"
    result = subprocess.run(
        ["python", merge_script],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Error running merge.py script. Error: {result.stderr}")

    print("Merge Script Output:")
    print(result.stdout)
