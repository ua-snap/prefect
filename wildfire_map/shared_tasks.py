from prefect import task
import subprocess


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
                f". $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env list | grep {conda_env_name}",
                shell=True,
            ).returncode
            == 0
        )

        if not conda_env_exists:
            print(f"Conda environment '{conda_env_name}' does not exist. Installing...")

            # Install the Conda environment from the environment file
            subprocess.run(
                f". $HOME/miniconda3/bin/activate && $HOME/miniconda3/bin/conda env create -n {conda_env_name} -f {conda_env_file}",
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
                f". /opt/miniconda3/bin/activate && /opt/miniconda3/bin/conda env list | grep {conda_env_name}",
                shell=True,
            ).returncode
            == 0
        )

        if not conda_env_exists:
            print(f"Conda environment '{conda_env_name}' does not exist. Installing...")

            # Install the Conda environment from the environment file
            subprocess.run(
                f". /opt/miniconda3/bin/activate && /opt/miniconda3/bin/conda env create -n {conda_env_name} -f {conda_env_file}",
                shell=True,
                check=True,
            )
            print(f"Conda environment '{conda_env_name}' installed successfully")
        else:
            print(f"Conda environment '{conda_env_name}' already exists.")
