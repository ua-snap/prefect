import os
import shutil
import glob
import netrc
import subprocess
import tarfile
import numpy as np
import pandas as pd
import geopandas as gpd
from urllib.parse import urlparse
from prefect import task
from seaice.seaice import SeaIceRaw


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

        # Copy data from source directory to destination directory
        shutil.copyfile(source_file, destination_directory)

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
def run_ingest(ingest_directory, ingest_file="ingest.json"):
    # Run the ingest command
    command = ["/opt/rasdaman/bin/wcst_import.sh", "-c", "0", ingest_file]
    env = {"LUTS_PATH": f"{ingest_directory}/luts.py"}
    result = subprocess.run(
        command, cwd=ingest_directory, env=env, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Error running the command. Error: {result.stderr}")

    print("Command Output:")
    print(result.stdout)


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
        commandb = "https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0051_gsfc_nasateam_seaice/final-gsfc/north/monthly/nt_{}{}_f17_v1.1_n.bin".format(
            str(year), month
        )
        os.system(commanda + commandb)


@task
def generate_annual_sea_ice_geotiffs(year, env_path, output_directory):
    os.environ["PROJ_LIB"] = f"{env_path}/proj"
    os.environ["GDAL_DATA"] = f"{env_path}/gdal"

    def seaicegen(fn, output_path):
        sic = SeaIceRaw(fn)

        # Collect the year and month from the file name from NSIDC data.
        year_month = str(os.path.basename(fn)).split("_")[1]

        # Output filename is generated with year + month.
        output_filename = os.path.join(
            output_path,
            f"seaice_conc_sic_mean_pct_monthly_panarctic_{year_month[0:4]}_{year_month[4:]}.tif",
        )

        # Run GDAL to warp file to match previous years in hsia_arctic_production
        # Rasdaman coverage.
        sic.to_gtiff_3572(output_filename)

    dat_path = os.path.join("/tmp/nsidc_raw", str(year))
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    l = glob.glob(os.path.join(dat_path, "*.bin"))
    _ = [seaicegen(fn, output_directory) for fn in l]
