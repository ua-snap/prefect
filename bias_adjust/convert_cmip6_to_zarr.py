"""Wrapper flow for converting a suite of CMIP6 data to Zarr format.
Uses fixed year range and assumes certain directory structure.
Hard wired for daily data.

"""

from prefect import flow, task
import paramiko
from pathlib import Path
from utils import utils, cmip6
from netcdf_to_zarr import run_netcdf_zarr_conversion

# Define your SSH parameters
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22

tmp_year_str = "{model}/{scenario}/day/{var_id}/{var_id}_day_{model}_{scenario}_regrid_{{year}}0101-{{year}}1231.nc"
tmp_zarr_name = "{var_id}_{model}_{scenario}.zarr"
start_year = 1965
end_year = 2014


@flow(log_prints=True)
def convert_cmip6_to_zarr(
    ssh_username,
    ssh_private_key_path,
    repo_name,
    branch_name,
    conda_env_name,
    cmip6_directory,
    vars,
    models,
    scenarios,
    scratch_directory,  # e.g. /import/beegfs/kmredilla
    write_dir_name,  # e.g. zarr_bias_adjust_inputs
):
    vars = cmip6.validate_vars(vars)
    models = cmip6.validate_models(models)
    scenarios = cmip6.validate_scenarios(scenarios)

    # Create an SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load the private key for key-based authentication
        private_key = paramiko.RSAKey(filename=ssh_private_key_path)

        # Connect to the SSH server using key-based authentication
        ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)

        repo_path = utils.clone_github_repository(
            ssh, repo_name, branch_name, scratch_directory
        )

        utils.check_for_nfs_mount(ssh, "/import/beegfs")

        utils.ensure_slurm(ssh)

        utils.ensure_conda(ssh)

        utils.ensure_conda_env(
            ssh, conda_env_name, repo_path.joinpath("environment.yml")
        )

        launcher_script = repo_path.joinpath("bias_adjust", "run_netcdf_to_zarr.py")
        write_directory = Path(scratch_directory).joinpath(write_dir_name)
        output_directory = write_directory.joinpath("zarr")
        slurm_directory = write_directory.joinpath("slurm")

        utils.create_directories(
            ssh, [write_directory, output_directory, slurm_directory]
        )

        job_ids = []
        for var_id in vars:
            for model in models:
                for scenario in scenarios:
                    year_str = tmp_year_str.format(
                        model=model, scenario=scenario, var_id=var_id
                    )
                    zarr_path = output_directory.joinpath(
                        tmp_zarr_name.format(
                            var_id=var_id, model=model, scenario=scenario
                        )
                    )
                    kwargs = {
                        "ssh": ssh,
                        "launcher_script": launcher_script,
                        "conda_env_name": conda_env_name,
                        "netcdf_dir": cmip6_directory,
                        "year_str": year_str,
                        "start_year": start_year,
                        "end_year": end_year,
                        "zarr_path": zarr_path,
                        "slurm_dir": slurm_directory,
                    }
                    job_ids.extend(run_netcdf_zarr_conversion(**kwargs))

        utils.wait_for_jobs_completion(
            ssh,
            job_ids,
            completion_message="Slurm jobs for Zarr conversion complete.",
        )

    finally:
        ssh.close()


if __name__ == "__main__":
    ssh_username = "snapdata"
    ssh_private_key_path = "/home/snapdata/.ssh/id_rsa"
    repo_name = "cmip6-utils"
    branch_name = "main"
    conda_env_name = "cmip6-utils"
    models = "all"
    scenarios = "all"
    vars = "tasmax pr"
    scratch_directory = "/import/beegfs/CMIP6/snapdata"
    write_dir_name = "zarr_bias_adjust_inputs"
    cmip6_directory = "/beegfs/CMIP6/snapdata/cmip6_4km_3338"

    convert_cmip6_to_zarr.serve(
        name="convert-cmip6-netcdf-to-zarr",
        tags=["Bias adjustment", "Downscaling"],
        parameters={
            "ssh_username": ssh_username,
            "ssh_private_key_path": ssh_private_key_path,
            "repo_name": repo_name,
            "branch_name": branch_name,
            "conda_env_name": conda_env_name,
            "scratch_directory": scratch_directory,
            "write_dir_name": write_dir_name,
            "models": models,
            "scenarios": scenarios,
            "vars": vars,
            "cmip6_directory": cmip6_directory,
        },
    )
