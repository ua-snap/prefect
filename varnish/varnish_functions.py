import os
import subprocess
import requests
from prefect import task


@task(name="Request API Re-cache URL")
def request_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(e)


@task(name="Clone GitHub Repository")
def clone_github_repository(branch, destination_directory):
    # Clone or pull GitHub repository
    target_directory = os.path.join(destination_directory, "data-api")
    directory_exists = os.path.isdir(target_directory)

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
            "https://github.com/ua-snap/data-api",
            destination_directory,
        ]
        result = subprocess.run(git_clone_command, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(
                f"Error cloning the GitHub repository. Error: {result.stderr}"
            )


@task(name="Install Conda Environment")
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


@task(name="Start Flask Server")
def start_flask_server(conda_env_name, application_path):
    """
    Task to start a Flask server using a specific Conda environment.
    """
    try:
        # Activate the Conda environment
        activate_env_command = (
            f"source /opt/miniconda3/bin/activate && conda activate {conda_env_name}"
        )
        subprocess.run(activate_env_command, shell=True, check=True)

        # Start the Flask server
        flask_command = f"python {application_path}"
        subprocess.Popen(flask_command, shell=True)

        print(f"Flask server started at http://localhost:5000/")
    except subprocess.CalledProcessError as e:
        print(f"Error starting Flask server: {e}")
