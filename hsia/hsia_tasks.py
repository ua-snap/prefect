import os
import shutil
import glob
import netrc
import subprocess
import tarfile
from urllib.parse import urlparse
from prefect import task
from seaice import netcdf_to_geotiff


@task
def check_for_nfs_mount(nfs_directory="/CKAN_Data"):
    # Check if NFS directory is mounted
    result = subprocess.run(
        ["df", "-h", "|", "grep", nfs_directory], capture_output=True, text=True
    )
    if not result.stdout:
        raise Exception(f"NFS directory '{nfs_directory}' is not mounted")


@task
def copy_data_from_nfs_mount(source_file, destination_directory):
    try:
        # Ensure destination directory exists
        os.makedirs(destination_directory, exist_ok=True)

        # Extract file name from source file path
        file_name = os.path.basename(source_file)

        # Construct destination file path
        destination_file = os.path.join(destination_directory, file_name)

        # Copy data from source directory to destination directory
        shutil.copyfile(source_file, destination_file)

    except Exception as e:
        raise Exception(
            f"Failed to copy data from NFS mount at {source_file}. Error: {str(e)}"
        )


@task
def untar_file(tar_file, data_directory):
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    if not os.path.exists(tar_file):
        raise Exception(f"Tar file {tar_file} does not exist")

    with tarfile.open(tar_file, "r:gz") as tar:
        tar.extractall(data_directory)


@task
def clone_github_repository(repo_name, branch, destination_directory):
    # Clone or pull GitHub repository
    target_directory = f"{destination_directory}/{repo_name}"
    result = subprocess.run(
        [f"if [ -d '{target_directory}' ]; then echo 'true'; else echo 'false'; fi"],
        shell=True,
        capture_output=True,
        text=True,
    )
    directory_exists = result.stdout.strip() == "true"

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
            f"https://github.com/ua-snap/{repo_name}",
        ]
        result = subprocess.run(
            git_clone_command, cwd=destination_directory, capture_output=True, text=True
        )
        if result.returncode != 0:
            raise Exception(
                f"Error cloning the GitHub repository. Error: {result.stderr}"
            )


@task
def check_for_netrc_file(netrc_home, netrc_contents):
    # Check for .netrc file
    result = subprocess.run(
        ["cat", f"{netrc_home}/.netrc"], capture_output=True, text=True
    )
    # If the .netrc file does not exist, create it
    if result.returncode != 0:
        with open(f"{netrc_home}/.netrc", "w") as f:
            f.write(netrc_contents)
    else:
        with open(f"{netrc_home}/.netrc", "r") as f:
            contents = f.read()
            if contents != netrc_contents:
                # If the .netrc file exists but the contents are different, overwrite it
                with open(f"{netrc_home}/.netrc", "w") as f:
                    f.write(netrc_contents)
    # Set the permissions on the .netrc file
    result = subprocess.run(
        ["chmod", "600", f"{netrc_home}/.netrc"], capture_output=True, text=True
    )


@task
def download_new_nsidc_data(year):
    # Change the year to match the year we want to download
    out_dir = f"/tmp/nsidc_raw/{year}"
    info = netrc.netrc()
    username, account, password = info.authenticators(
        urlparse("https://urs.earthdata.nasa.gov").hostname
    )

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    for month in ["{:02d}".format(month + 1) for month in range(12)]:
        commanda = 'wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies --no-check-certificate --auth-no-challenge=on -r --reject "index.html*" -np -nd -e robots=off --user {} --password "{}" -P {} '.format(
            username, password, out_dir
        )
        commandb = "https://n5eil01u.ecs.nsidc.org/PM/NSIDC-0051.002/{}.{}.01/NSIDC0051_SEAICE_PS_N25km_{}{}_v2.0.nc".format(
            str(year), month, str(year), month
        )
        os.system(commanda + commandb)


@task
def generate_annual_sea_ice_geotiffs(year, output_directory):
    # Generate annual Sea Ice GeoTIFFs
    for month in range(1, 13):
        input_netcdf = (
            f"/tmp/nsidc_raw/{year}/NSIDC0051_SEAICE_PS_N25km_{year}{month:02d}_v2.0.nc"
        )
        output_tiff = f"{output_directory}/seaice_conc_sic_mean_pct_monthly_panarctic_{year}_{month:02d}.tif"
        netcdf_to_geotiff(input_netcdf, output_tiff)


@task
def tar_directory(directory, output_file):
    with tarfile.open(output_file, "w:gz") as tar:
        print("Creating new tar file of Sea Ice data...")
        tar.add(directory, arcname=os.path.basename(directory))
    return output_file


@task
def copy_tarfile_to_nfs_mount(tar_file, source_directory):
    print("Copying tar file to NFS mount...")
    shutil.copyfile(tar_file, source_directory)
