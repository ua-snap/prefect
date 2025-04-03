from prefect import task
import subprocess
import datetime
import requests
import pandas as pd
from pathlib import Path
import s3fs
import xarray as xr
import numpy as np
from scipy.interpolate import griddata
import matplotlib as mpl
from matplotlib import pyplot as plt
from cartopy import crs as ccrs
from osgeo import gdal


def run_in_conda_env(command, env_name="viirs_smoke"):
    """Helper function to run commands in a specific conda environment"""
    full_command = f"source /opt/miniconda3/bin/activate {env_name} && {command}"
    process = subprocess.Popen(
        full_command,
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


@task
def find_jpss_observation_times(
    observation_date, lower_left_lat_lon, upper_right_lat_lon, sat_name
):
    # Convert entered observation_date to format needed by UWisc OrbNav API
    api_date = datetime.datetime.strptime(observation_date, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )

    # Set JPSS satellite URL for UWisc OrbNav API
    sat_number_dict = {"SNPP": "37849", "NOAA20": "43013", "NOAA21": "54234"}
    sat_number = sat_number_dict.get(sat_name)
    url = (
        f"http://sips.ssec.wisc.edu/orbnav/api/v1/boxtimes.json?start="
        f"{api_date}T00:00:00Z&sat={sat_number}&end={api_date}T23:59:59Z&ur="
        f"{upper_right_lat_lon}&ll={lower_left_lat_lon}"
    )

    # Use requests library to get json response from UWisc OrbNav API
    response = requests.get(url)
    data = response.json()

    # Convert json response values from "data" key into a dataframe
    df = pd.DataFrame(data["data"], columns=["enter", "leave"])

    # Make two new dataframes, for "enter" and "leave" column lists
    df_enter = pd.DataFrame(
        df["enter"].to_list(),
        columns=[
            "enter_datetime",
            "enter_lat",
            "enter_lon",
            "enter_sea",
            "enter_orbit",
        ],
    )
    df_leave = pd.DataFrame(
        df["leave"].to_list(),
        columns=[
            "leave_datetime",
            "leave_lat",
            "leave_lon",
            "leave_sea",
            "leave_orbit",
        ],
    )

    # Combine "enter" & "leave" dataframes into new dataframe; drop extra columns
    combined = pd.concat([df_enter, df_leave], axis=1, join="outer").drop(
        columns=[
            "enter_lat",
            "enter_lon",
            "enter_sea",
            "leave_lat",
            "leave_lon",
            "leave_sea",
        ],
        axis=1,
    )

    # Drop rows with descending orbits
    combined.drop(
        combined[
            (combined["leave_orbit"] == "D") | (combined["enter_orbit"] == "D")
        ].index,
        inplace=True,
    )

    # Export the "enter_datetime" & "leave_datetime" columns to lists
    enter_list = combined["enter_datetime"].tolist()
    leave_list = combined["leave_datetime"].tolist()

    # Remove the colon from the list of enter/leave times (strings)
    start_times = [time[11:16].replace(":", "") for time in enter_list]
    end_times = [time[11:16].replace(":", "") for time in leave_list]

    return start_times, end_times


@task(name="Install Purple Air Conda Environment")
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


@task(name="Execute Local Script")
def execute_local_script(script_path, output_path, conda_env_name="fire_map"):
    # Execute the script on the local machine
    process = subprocess.Popen(
        f". /opt/miniconda3/bin/activate {conda_env_name}; python {script_path} --out-dir {output_path}",
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


@task
def query_nodd_viirs_adp(fs, observation_date, sat_name, start_times, end_times):
    # Run the query in the conda environment
    year = observation_date[:4]
    month = observation_date[4:6]
    day = observation_date[6:]
    keys = ["SNPP", "NOAA20", "NOAA21"]
    values = [
        "noaa-nesdis-snpp-pds/VIIRS-JRR-ADP/",
        "noaa-nesdis-n20-pds/VIIRS-JRR-ADP/",
        "noaa-nesdis-n21-pds/VIIRS-JRR-ADP/",
    ]
    abbreviation_dictionary = {keys[i]: values[i] for i in range(len(keys))}
    product_path = abbreviation_dictionary.get(sat_name)
    try:
        day_files = fs.ls(
            product_path + year + "/" + month + "/" + day + "/", refresh=True
        )
    except:
        day_files = []
    if day_files:
        nodd_file_list = [
            file
            for start_time, end_time in zip(start_times, end_times)
            for file in day_files
            if (
                file.split("/")[-1].split("_")[3][9:13] >= start_time
                and file.split("/")[-1].split("_")[3][9:13] <= end_time
            )
        ]
    else:
        nodd_file_list = []
    return nodd_file_list


@task
def process_viirs_adp_saai_smoke(ds):
    # Process the data in the conda environment
    with np.errstate(invalid="ignore"):
        pqi4 = ds.PQI4.to_masked_array().astype("int8")
        saai_smoke = ds.SAAI.where(ds.Smoke == 1).to_masked_array().astype("float32")
    smoke_algorithm_mask = ((pqi4 & 16 == 16) & (pqi4 & 32 != 32)) | (
        (pqi4 & 16 != 16) & (pqi4 & 32 == 32)
    )
    saai_smoke = np.ma.masked_where(smoke_algorithm_mask, saai_smoke)
    return saai_smoke


@task
def interpolate_missing_pixels(da):
    # Interpolate in the conda environment
    height, width = da.values.shape
    xx, yy = np.meshgrid(np.arange(width), np.arange(height))
    mask = np.isnan(da)
    known_x = xx[np.logical_not(mask)]
    known_y = yy[np.logical_not(mask)]
    missing_x = xx[mask]
    missing_y = yy[mask]
    known_v = da.to_masked_array().compressed()
    interpolated_values = griddata(
        (known_x, known_y), known_v, (missing_x, missing_y), method="nearest"
    )
    da_interpolated = da.values.copy()
    da_interpolated[missing_y, missing_x] = interpolated_values
    return da_interpolated


@task
def plot_simple_viirs_adp_smoke(fs, file_list, png_domain, save_path):
    # Create the plot in the conda environment
    fig = plt.figure(figsize=(8, 10))
    ax = plt.axes(projection=ccrs.Mercator(central_longitude=180))
    plt.axis("off")
    ax.set_extent(png_domain, crs=ccrs.PlateCarree())
    norm = mpl.colors.Normalize(vmin=0, vmax=2)
    cmap = plt.get_cmap("PuRd")

    for file in file_list:
        print("Now processing", file.split("/")[-1])
        with fs.open(file, mode="rb") as remote_file:
            with xr.open_dataset(remote_file, engine="h5netcdf") as ds:
                saai_smoke = process_viirs_adp_saai_smoke(ds)
                latitude_interpolated = interpolate_missing_pixels(ds.Latitude)
                longitude_interpolated = interpolate_missing_pixels(ds.Longitude)
                ax.pcolormesh(
                    longitude_interpolated,
                    latitude_interpolated,
                    saai_smoke,
                    cmap=cmap,
                    norm=norm,
                    transform=ccrs.PlateCarree(),
                )

    save_name = "viirs_adp_saai_smoke"
    fig.savefig(save_path / save_name, transparent=True, dpi=300, bbox_inches="tight")
    plt.close()
    return save_name


@task
def create_geotiff(output_file, input_file, tif_domain):
    # Create the GeoTIFF in the conda environment
    translate_file = gdal.Translate(
        output_file,
        input_file,
        outputBounds=tif_domain,
        outputSRS="EPSG:4326",
        format="GTiff",
    )
    translate_file = None
