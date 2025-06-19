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


@task(name="Execute Smokey Bear Local Script")
def execute_local_script(
    script_path,
    output_directory,
    conda_env_name="smokeybear",
    conda_local_environment=False,
):
    if conda_local_environment:
        conda_source = ". $HOME/miniconda3/bin/activate"
    else:
        conda_source = ". /opt/miniconda3/bin/activate"
    process = subprocess.Popen(
        f"{conda_source}; conda activate {conda_env_name}; {script_path} -o {output_directory}",
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
