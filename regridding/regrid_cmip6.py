from prefect import flow
import paramiko
from pathlib import Path

import regridding_functions

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22


@flow(log_prints=True)
def regrid_cmip6(
    ssh_username,
    ssh_private_key_path,
    branch_name,
    cmip6_directory,
    scratch_directory,
    slurm_email,
    no_clobber,
    generate_batch_files,
    vars,
):
    vars = regridding_functions.validate_vars(vars)

    # build additional parameters from prefect inputs
    repo_regridding_directory = f"{scratch_directory}/cmip6-utils/regridding"
    conda_init_script = f"{scratch_directory}/cmip6-utils/regridding/conda_init.sh"
    regrid_script = f"{scratch_directory}/cmip6-utils/regridding/regrid.py"
    slurm_script = f"{scratch_directory}/cmip6-utils/regridding/slurm.py"
    generate_batch_files_script = (
        f"{scratch_directory}/cmip6-utils/regridding/generate_batch_files.py"
    )
    run_generate_batch_files_script = (
        f"{scratch_directory}/cmip6-utils/regridding/run_generate_batch_files.py"
    )
    run_qc_script = f"{scratch_directory}/cmip6-utils/regridding/run_qc.py"
    qc_script = f"{scratch_directory}/cmip6-utils/regridding/qc.py"
    visual_qc_notebook = f"{scratch_directory}/cmip6-utils/regridding/qc.ipynb"
    output_directory = f"{scratch_directory}/cmip6_regridding"
    regrid_dir = f"{output_directory}/regrid"
    regrid_batch_dir = f"{output_directory}/regrid_batch"
    slurm_dir = f"{output_directory}/slurm"

    # target regridding file - all files will be regridded to the grid in this file
    target_grid_fp = f"{cmip6_directory}/ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/Amon/tas/gn/v20200528/tas_Amon_CESM2_ssp370_r11i1p1f1_gn_206501-210012.nc"

    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        regridding_functions.clone_github_repository(
            ssh, branch_name, scratch_directory
        )

        regridding_functions.check_for_nfs_mount(ssh, "/import/beegfs")

        regridding_functions.install_conda_environment(
            ssh, "cmip6-utils", f"{scratch_directory}/cmip6-utils/environment.yml"
        )

        if generate_batch_files == True:
            regridding_functions.run_generate_batch_files(
                ssh,
                conda_init_script,
                generate_batch_files_script,
                run_generate_batch_files_script,
                cmip6_directory,
                regrid_batch_dir,
                slurm_email,
                vars,
            )

            job_ids = regridding_functions.get_job_ids(ssh, ssh_username)

            regridding_functions.wait_for_jobs_completion(ssh, job_ids)

        regridding_functions.create_and_run_slurm_scripts(
            ssh,
            slurm_script,
            slurm_dir,
            regrid_dir,
            regrid_batch_dir,
            slurm_email,
            conda_init_script,
            regrid_script,
            target_grid_fp,
            no_clobber,
        )

        job_ids = regridding_functions.get_job_ids(ssh, ssh_username)

        regridding_functions.wait_for_jobs_completion(ssh, job_ids)

        regridding_functions.run_qc(ssh, output_directory, cmip6_directory, repo_regridding_directory, conda_init_script, run_qc_script, qc_script, visual_qc_notebook, vars, slurm_email)

        job_ids = regridding_functions.get_job_ids(ssh, ssh_username)

        regridding_functions.wait_for_jobs_completion(ssh, job_ids)

    finally:
        ssh.close()


if __name__ == "__main__":
    # prefect parameter inputs
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    branch_name = "main"
    cmip6_directory = Path("/beegfs/CMIP6/arctic-cmip6/CMIP6")
    scratch_directory = Path(f"/center1/CMIP6/snapdata/")
    slurm_email = "uaf-snap-sys-team@alaska.edu"
    no_clobber = False
    generate_batch_files = True
    vars = "all"

    regrid_cmip6.serve(
        name="regrid-cmip6",
        tags=["CMIP6 Regridding"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "branch_name": branch_name,
            "cmip6_directory": cmip6_directory,
            "scratch_directory": scratch_directory,
            "slurm_email": slurm_email,
            "no_clobber": no_clobber,
            "generate_batch_files": generate_batch_files,
            "vars": vars,
        },
    )
