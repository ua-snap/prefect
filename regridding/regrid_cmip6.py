"""Flow for regridding CMIP6 data

Regridded data is written to <scratch_dir>/<work_dir_name>/regrid
"""

from prefect import flow
import paramiko
from pathlib import Path
from regridding import regridding_functions as rf
from utils import utils

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22

# name of folder in working_dir where regridded data is written
out_dir_name = "regrid"


@flow(log_prints=True)
def regrid_cmip6(
    ssh_host,
    ssh_port,
    ssh_username,
    ssh_private_key_path,
    repo_name,  # cmip6-utils
    branch_name,
    cmip6_dir,
    target_grid_file,
    scratch_dir,
    work_dir_name,
    out_dir_name,
    no_clobber,
    variables,
    interp_method,
    freqs,
    models,
    scenarios,
    conda_env_name,
    rasdafy,
    target_sftlf_fp=None,
    partition="t2small",
):
    variables = rf.validate_vars(variables)
    freqs = rf.validate_freqs(freqs)
    models = rf.validate_models(models)
    scenarios = rf.validate_scenarios(scenarios)

    # build additional parameters from prefect inputs
    repo_regridding_dir = f"{scratch_dir}/cmip6-utils/regridding"
    regrid_script = f"{scratch_dir}/cmip6-utils/regridding/regrid.py"
    slurm_script = f"{scratch_dir}/cmip6-utils/regridding/slurm.py"
    generate_batch_files_script = (
        f"{scratch_dir}/cmip6-utils/regridding/generate_batch_files.py"
    )
    run_generate_batch_files_script = (
        f"{scratch_dir}/cmip6-utils/regridding/run_generate_batch_files.py"
    )
    run_qc_script = f"{scratch_dir}/cmip6-utils/regridding/run_qc.py"
    qc_notebook = f"{scratch_dir}/cmip6-utils/regridding/qc.ipynb"
    working_dir = f"{scratch_dir}/{work_dir_name}"
    slurm_dir = f"{working_dir}/slurm"
    output_dir = f"{working_dir}/{out_dir_name}"
    regrid_batch_dir = f"{slurm_dir}/regrid_batch_files"

    # target regridding file - all files will be regridded to the grid in this file
    # target_grid_fp = f"{cmip6_dir}/ScenarioMIP/NCAR/CESM2/ssp370/r11i1p1f1/Amon/tas/gn/v20200528/tas_Amon_CESM2_ssp370_r11i1p1f1_gn_206501-210012.nc"
    # target_sftlf_fp =

    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        repo_path = utils.clone_github_repository(
            ssh, repo_name, branch_name, scratch_dir
        )

        utils.check_for_nfs_mount(ssh, "/import/beegfs")

        utils.ensure_slurm(ssh)

        utils.ensure_conda(ssh)

        utils.ensure_conda_env(
            ssh, conda_env_name, repo_path.joinpath("environment.yml")
        )

        batch_job_ids = rf.run_generate_batch_files(
            ssh,
            conda_env_name,
            generate_batch_files_script,
            run_generate_batch_files_script,
            cmip6_dir,
            regrid_batch_dir,
            variables,
            freqs,
            models,
            scenarios,
        )

        utils.wait_for_jobs_completion(
            ssh,
            batch_job_ids,
            completion_message="Slurm jobs for batch file generation complete.",
        )

        run_regrid_kwargs = {
            "ssh": ssh,
            "slurm_script": slurm_script,
            "slurm_dir": slurm_dir,
            "regrid_dir": output_dir,
            "regrid_batch_dir": regrid_batch_dir,
            "conda_env_name": conda_env_name,
            "regrid_script": regrid_script,
            "target_grid_file": target_grid_file,
            "no_clobber": no_clobber,
            "variables": variables,
            "interp_method": interp_method,
            "freqs": freqs,
            "models": models,
            "scenarios": scenarios,
            "rasdafy": rasdafy,
            "partition": partition,
            "target_sftlf_fp": target_sftlf_fp,
        }
        regrid_job_ids = rf.run_regridding(**run_regrid_kwargs)

        utils.wait_for_jobs_completion(
            ssh,
            regrid_job_ids,
            completion_message="Slurm jobs for regridding complete.",
        )

        qc_job_ids = rf.run_qc(
            ssh,
            output_dir,
            cmip6_dir,
            repo_regridding_dir,
            conda_env_name,
            run_qc_script,
            qc_notebook,
            variables,
            freqs,
            models,
            scenarios,
        )

        utils.wait_for_jobs_completion(ssh, qc_job_ids, "Slurm jobs for QC complete.")

    finally:
        ssh.close()

    return output_dir


if __name__ == "__main__":
    # prefect parameter inputs
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    cmip6_dir = "/beegfs/CMIP6/arctic-cmip6/CMIP6"
    target_grid_file = "/beegfs/CMIP6/kmredilla/downscaling/era5_target_slice.nc"
    scratch_dir = "/beegfs/CMIP6/snapdata/"
    work_dir_name = "cmip6_regridding"
    no_clobber = False
    variables = "all"
    interp_method = "bilinear"
    freqs = "all"
    models = "all"
    scenarios = "all"
    conda_env_name = "cmip6-utils"
    rasdafy = True

    regrid_cmip6.serve(
        name="regrid-cmip6",
        tags=["CMIP6 Regridding"],
        parameters={
            "ssh_host": ssh_host,
            "ssh_port": ssh_port,
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "cmip6_dir": cmip6_dir,
            "work_dir_name": work_dir_name,
            "out_dir_name": out_dir_name,
            "scratch_dir": scratch_dir,
            "no_clobber": no_clobber,
            "variables": variables,
            "interp_method": interp_method,
            "freqs": freqs,
            "models": models,
            "scenarios": scenarios,
            "conda_env_name": conda_env_name,
            "rasdafy": rasdafy,
        },
    )
