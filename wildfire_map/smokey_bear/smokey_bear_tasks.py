import os
from prefect import task
import subprocess


@task
def check_for_admin_pass(target_directory, admin_password):
    file_path = f"{target_directory}/.adminpass"
    admin_pass = f'export admin_pass="{admin_password}"\n'

    if not os.path.exists(file_path):
        # Only gets here if the file doesn't exist or the password is wrong
        with open(file_path, "w") as file:
            file.write(admin_pass)
            return True
    # Read the content of the local file
    with open(file_path, "r") as file:
        content = file.read()

    # Does the admin password match the supplied admin_password variable?
    if admin_pass in content:
        return True
    else:
        with open(file_path, "w") as file:
            file.write(admin_pass)
            return True


@task(name="Install Smokey Bear Conda Environment")
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


@task(name="Execute Smokey Bear Local Script")
def execute_local_script(script_path):
    # Execute the script on the local machine
    process = subprocess.Popen(
        f"sudo {script_path}",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    exit_code = process.returncode

    output = stdout.decode("utf-8")
    errors = stderr.decode("utf-8")

    if exit_code == 0:
        print(f"Processing output: {errors}")
        print(f"Final output of the script {script_path}: {output}")
        print(f"Script {script_path} executed successfully.")
    else:
        print(f"Error occurred while executing the script {script_path}.")
        print(f"Error output: {errors}")

    return exit_code, output, errors
