from pathlib import Path
import paramiko
from prefect import flow, task, get_run_logger

import curation_functions


# SSH connection details for Chinook HPC
SSH_HOST = "chinook04.rcs.alaska.edu"
SSH_PORT = 22


@flow(name="era5-processing",
      description="Orchestrate ERA5 data processing on Chinook HPC",
      log_prints=True)
def submit_era5_jobs(
    ssh_username: str,
    ssh_private_key_path: Path,
    branch_name: str,
    working_directory: Path,
    variables: str,
    ERA5_INPUT_DIR: Path,
    ERA5_OUTPUT_DIR: Path,
    start_year: int,
    end_year: int,
    max_concurrent: int,
    overwrite: bool = False,
    no_retry: bool = False
):
    logger = get_run_logger()

    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)
        # Connect to the SSH server using key-based authentication
        ssh.connect(SSH_HOST, SSH_PORT, ssh_username, pkey=private_key)

        curation_functions.clone_github_repository(ssh, branch_name, working_directory)

        curation_functions.check_for_nfs_mount(ssh, "/import/beegfs")

        curation_functions.install_conda_environment(
            ssh, "snap-geo", f"{working_directory}/wrf-downscaled-era5-curation/environment.yml"
        )

        repo_path = working_directory / "wrf-downscaled-era5-curation"
        
        @task
        def build_and_run_job_submission_script():
            cmd = (
                f"export ERA5_INPUT_DIR={ERA5_INPUT_DIR} && "
                f"export ERA5_OUTPUT_DIR={ERA5_OUTPUT_DIR} && "
                f"conda activate snap-geo && "
                f"cd {repo_path} && "
                f"python submit_era5_jobs.py "
                f"--variables {variables} "
                f"--start_year {start_year} "
                f"--end_year {end_year} "
                f"--max_concurrent {max_concurrent} "
            )
    
            if overwrite:
                cmd += "--overwrite "
            if no_retry:
                cmd += "--no_retry"
    
            logger.info(f"Executing submission command: {cmd}")

            stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                error_output = stderr.read().decode("utf-8")
                raise Exception(f"Error submitting jobs: {error_output}")
            else:
                logger.info("Jobs submitted successfully")

        build_and_run_job_submission_script()

    except Exception as e:
        logger.error(f"Flow failed: {str(e)}")
        raise
    finally:
        ssh.close()
        logger.info("SSH connection closed")

if __name__ == "__main__":
    submit_era5_jobs.serve(
        parameters={
            "ssh_username": "snapdata",
            "ssh_private_key_path": "/Users/cparr/.ssh/id_rsa",
            "branch_name": "batch_io",
            "working_directory": Path("/beegfs/CMIP6/snapdata"),
            "variables": "t2_mean,t2_min,t2_max",
            "ERA5_INPUT_DIR": Path("/beegfs/CMIP6/wrf_era5/04km"),
            "ERA5_OUTPUT_DIR": Path("/beegfs/CMIP6/snapdata/curated_wrf_era5-04km"),
            "start_year": 1960,
            "end_year": 2019,
            "max_concurrent": 60,
            "overwrite": False,
            "no_retry": False,
        }
    )