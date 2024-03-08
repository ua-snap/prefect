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
):
    
    # build additional parameters from prefect inputs
    conda_init_script = f"{scratch_directory}/cmip6-utils/regridding/conda_init.sh"
    regrid_script = scratch_directory.joinpath("regridding/regrid.py")
    slurm_script = scratch_directory.joinpath("regridding/slurm.py")
    generate_batch_files_script = scratch_directory.joinpath("regridding/generate_batch_files.py")
    regrid_dir = scratch_directory.joinpath("regrid")
    regrid_batch_dir = scratch_directory.joinpath("regrid_batch")
    slurm_dir = scratch_directory.joinpath("slurm")

    # target regridding file - all files will be regridded to the grid in this file
    target_grid_fp = cmip6_directory.joinpath("ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/Amon/tas/gn/v20200528/tas_Amon_CESM2_ssp370_r11i1p1f1_gn_206501-210012.nc")

    # make these dirs if they don't exist
    regrid_dir.mkdir(exist_ok=True)
    regrid_batch_dir.mkdir(exist_ok=True)
    slurm_dir.mkdir(exist_ok=True)

    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        regridding_functions.clone_github_repository(ssh, branch_name, scratch_directory)

        regridding_functions.check_for_nfs_mount(ssh, "/import/beegfs")

        regridding_functions.install_conda_environment(ssh, "cmip6-utils", f"{scratch_directory}/cmip6-utils/environment.yml")

        regridding_functions.generate_batch_files(ssh, conda_init_script, generate_batch_files_script, cmip6_directory, regrid_batch_dir, slurm_email)

        job_ids = regridding_functions.get_job_ids(ssh, ssh_username)

        regridding_functions.wait_for_jobs_completion(ssh, job_ids)

        regridding_functions.create_and_run_slurm_scripts(ssh, 
                                                          slurm_script, 
                                                          slurm_dir,
                                                          regrid_dir,
                                                          regrid_batch_dir,
                                                          slurm_email,
                                                          conda_init_script,
                                                          regrid_script,
                                                          target_grid_fp,
                                                          no_clobber)

        job_ids = regridding_functions.get_job_ids(ssh, ssh_username)

        regridding_functions.wait_for_jobs_completion(ssh, job_ids)

        #regridding_functions.qc(ssh, working_directory, input_dir)

        #regridding_functions.visual_qc_nb(ssh, working_directory, input_dir)

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
        },
    )
