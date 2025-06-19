from prefect import task
import subprocess


@task(name="Execute Fire Layers Local Script")
def execute_local_script(
    script_path, output_path, conda_env_name="fire_map", conda_local_environment=False
):
    # Execute the script on the local machine
    if conda_local_environment:
        conda_source = ". $HOME/miniconda3/bin/activate"
    else:
        conda_source = ". /opt/miniconda3/bin/activate"
    process = subprocess.Popen(
        f"{conda_source}; conda activate {conda_env_name}; python {script_path} --out-dir {output_path}",
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
        print(f"Error output: {errors}")
        raise Exception(
            f"Error occurred while executing the script {script_path}. Error: {errors}"
        )

    return exit_code, output, errors
