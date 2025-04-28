"""
The script is designed to be used with Prefect to create a GeoTIFF from a VIIRS ADP map image file.

Requires an output directory to be passed in as an argument.
"""

from pathlib import Path

import datetime
from datetime import date

import s3fs

import requests

import pandas as pd

import xarray as xr

import numpy as np

from scipy.interpolate import griddata

import matplotlib as mpl
from matplotlib import pyplot as plt

from cartopy import crs as ccrs

from osgeo import gdal

import argparse


# Find start/end times for JPSS satellite overpass(es) of geographic domain
# Uses UWisc OrbNav API
def find_jpss_observation_times(
    observation_date, lower_left_lat_lon, upper_right_lat_lon, sat_name
):

    # Convert entered observation_date to format needed by UWisc OrbNav API
    api_date = date.isoformat(datetime.datetime.strptime(observation_date, "%Y%m%d"))

    # Set JPSS satellite URL for UWisc OrbNav API
    sat_number_dict = {"SNPP": "37849", "NOAA20": "43013", "NOAA21": "54234"}
    sat_number = sat_number_dict.get(sat_name)
    # Break long url string using f-string formatting
    url = (
        f"http://sips.ssec.wisc.edu/orbnav/api/v1/boxtimes.json?start="
        f"{api_date}T00:00:00Z&sat={sat_number}&end={api_date}T23:59:59Z&ur="
        f"{upper_right_lat_lon}&ll={lower_left_lat_lon}"
    )

    # Use requests library to get json response from UWisc OrbNav API
    response = requests.get(url)
    data = response.json()

    # Convert json response values from "data" key into a dataframe
    # "enter" & "leave": times when satellite enters/leaves domain bounding box
    df = pd.DataFrame(data["data"], columns=["enter", "leave"])

    # Make two new dataframes, for "enter" and "leave" column lists
    # Read in all the values in the lists as separate columns in new dataframes
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
    # Need 'HHMM' format for use with satellite data file names
    start_times = [time[11:16].replace(":", "") for time in enter_list]
    end_times = [time[11:16].replace(":", "") for time in leave_list]

    return start_times, end_times


# Query AWS NODD for available VIIRS operational ADP EDR data files
def query_nodd_viirs_adp(fs, observation_date, sat_name, start_times, end_times):

    # Define terms in directory paths on AWS NODD
    year = observation_date[:4]
    month = observation_date[4:6]
    day = observation_date[6:]
    # Make dictionary for JPSS satellite/ADP directory paths on AWS NODD
    # Keys for JPSS satellites
    keys = ["SNPP", "NOAA20", "NOAA21"]
    # Values for operational VIIRS ADP EDR directory paths
    values = [
        "noaa-nesdis-snpp-pds/VIIRS-JRR-ADP/",
        "noaa-nesdis-n20-pds/VIIRS-JRR-ADP/",
        "noaa-nesdis-n21-pds/VIIRS-JRR-ADP/",
    ]
    # Combine "values" and "keys" lists
    abbreviation_dictionary = {keys[i]: values[i] for i in range(len(keys))}
    product_path = abbreviation_dictionary.get(sat_name)

    # Query AWS NODD for available files for entire day
    try:
        day_files = fs.ls(
            product_path + year + "/" + month + "/" + day + "/", refresh=True
        )
    except:
        day_files = []

    if day_files:
        # Generate list of available files for observation time period(s)
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


# Process VIIRS ADP SAAI (Smoke only)
def process_viirs_adp_saai_smoke(ds):

    # Convert xarray DataArrays to NumPy masked arrays with correct dtype
    # Select "smoke present" (Smoke = 1) pixels
    # Casting xarray float32 to int8 gives NumPy error for fill value (NaN)
    # Silence "invalid value encountered in cast" warning with np.errstate
    with np.errstate(invalid="ignore"):
        pqi4 = ds.PQI4.to_masked_array().astype("int8")
        saai_smoke = ds.SAAI.where(ds.Smoke == 1).to_masked_array().astype("float32")

    # Select deep-blue based algorithm smoke pixels using PQI4 bits 4-5
    # Mask missing and IR-visible path pixels (see Table 8 in User's Guide)
    # missing (10): 16 + 0 = 16, IR-visible (01): 0 + 32 = 32
    smoke_algorithm_mask = ((pqi4 & 16 == 16) & (pqi4 & 32 != 32)) | (
        (pqi4 & 16 != 16) & (pqi4 & 32 == 32)
    )
    saai_smoke = np.ma.masked_where(smoke_algorithm_mask, saai_smoke)

    return saai_smoke


# Interpolate missing pixels in DataArray using SciPy
def interpolate_missing_pixels(da):

    # Height (rows) and width (columns) of DataArray
    height, width = da.values.shape

    # Create 2D arrays with dimensions of x (width) and y (height)
    xx, yy = np.meshgrid(np.arange(width), np.arange(height))

    # Boolean array of DataArray with masked (NaN) pixels set to "True"
    mask = np.isnan(da)

    # 1D arrays of known (non-masked) x, y indices (locations)
    # np.logical_not() reverses the boolean array
    known_x = xx[np.logical_not(mask)]
    known_y = yy[np.logical_not(mask)]

    # 1D arrays of missing (masked) x, y indices (locations)
    missing_x = xx[mask]
    missing_y = yy[mask]

    # 1D array of known (non-masked) DataArray values
    known_v = da.to_masked_array().compressed()

    # Interpolate missing DataArray values using SciPy (returns 1D array)
    interpolated_values = griddata(
        (known_x, known_y), known_v, (missing_x, missing_y), method="nearest"
    )

    # Assign interpolated values to indexed DataArray (replace NaNs)
    da_interpolated = da.values.copy()  # Copy of DataArray as np array
    da_interpolated[missing_y, missing_x] = interpolated_values

    return da_interpolated


# Plot processed SAAI Smoke from multiple VIIRS granule files on simple map
# No borderlines/coastlines, transparent background (for GeoTIFF)
# Files are opened remotely on the AWS NODD (not downloaded)


def plot_simple_viirs_adp_smoke(fs, file_list, png_domain, save_path):

    # Set up figure in matplotlib
    fig = plt.figure(figsize=(8, 10))

    # Set map projection using cartopy
    # Set central_longitude=180 for Alaska; avoids errors crossing antimeridian
    ax = plt.axes(projection=ccrs.Mercator(central_longitude=180))

    # Remove border around figure (for GeoTIFF)
    plt.axis("off")

    # Set geographic domain of map for image file (.png)
    # [W_lon, E_lon, S_lat, N_lat]
    ax.set_extent(png_domain, crs=ccrs.PlateCarree())

    # Set colormaps & normalization for plotting data
    norm = mpl.colors.Normalize(vmin=0, vmax=2)
    cmap = plt.get_cmap("PuRd")

    # Loop through VIIRS ADP files
    for file in file_list:
        print("Now processing", file.split("/")[-1])  # Print the file name

        # Open remote file using S3fs & xarray (automatically closes file when done)
        with fs.open(file, mode="rb") as remote_file:
            with xr.open_dataset(remote_file, engine="h5netcdf") as ds:

                # Process VIIRS ADP SAAI (smoke only)
                saai_smoke = process_viirs_adp_saai_smoke(ds)

                # Interpolate missing latitude & longitude values
                latitude_interpolated = interpolate_missing_pixels(ds.Latitude)
                longitude_interpolated = interpolate_missing_pixels(ds.Longitude)

                # Plot data
                ax.pcolormesh(
                    longitude_interpolated,
                    latitude_interpolated,
                    saai_smoke,
                    cmap=cmap,
                    norm=norm,
                    transform=ccrs.PlateCarree(),
                )

    save_name = "viirs_adp.png"

    # Save image file to designated directory
    # Set background as transparent (for GeoTIFF)
    fig.savefig(save_path / save_name, transparent=True, dpi=300, bbox_inches="tight")

    # Close plot
    plt.close()


# Create GeoTIFF from map image file
def create_geotiff(output_file, input_file, tif_domain):

    translate_file = gdal.Translate(
        output_file,
        input_file,
        outputBounds=tif_domain,
        outputSRS="EPSG:4326",
        format="GTiff",
    )
    translate_file = None  # Release memory used to generate .tif file


def main(output_directory=Path.cwd()):
    # Enter search variables for VIIRS ADP EDR data files on AWS NODD
    sat_name = "NOAA21"  #  Satellite name: 'SNPP', 'NOAA20', 'NOAA21'
    # observation_date = "20240627"  # 'YYYYMMDD' (string)
    observation_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    )  # 'YYYYMMDD' (string)

    ###############################################################################

    # Connect to AWS S3 anonymously
    fs = s3fs.S3FileSystem(anon=True)

    # Find the overpass start/end times for Alaska domain - east of Antimeridian
    east_start_times, east_end_times = find_jpss_observation_times(
        observation_date, "45,-180", "75,-130", sat_name
    )

    # Query AWS NODD for available files for Alaska domain - east of Antimeridian
    east_file_list = query_nodd_viirs_adp(
        fs, observation_date, sat_name, east_start_times, east_end_times
    )

    # Find "tomorrow's" 8-digit date as a string
    # Date for VIIRS granules falling west of Antimeridian is "tomorrow" in UTC
    obs_date = datetime.datetime.strptime(observation_date, "%Y%m%d").date()
    tomorrow = obs_date + datetime.timedelta(days=1)
    tomorrow = tomorrow.strftime("%Y%m%d")

    # Find the overpass start/end times for Alaska domain - west of Antimeridian
    west_start_times, west_end_times = find_jpss_observation_times(
        tomorrow, "45,170", "75,180", sat_name
    )

    # Query AWS NODD for available files for Alaska domain - west of Antimeridian
    west_file_list = query_nodd_viirs_adp(
        fs, tomorrow, sat_name, west_start_times, west_end_times
    )

    # Combine files for Alaska domains east & west of Antimeridian
    file_list = east_file_list + west_file_list

    # Print the available file names (optional sanity check)
    for file in file_list:
        print(file.split("/")[-1])

    # Plot smoke SAAI on a simple, transparent map & save locally as a .png file

    # ENTER USER SETTINGS

    # Enter domain for map image file
    # For Alaska, enter domain longitude in 360 degrees (i.e., 100°W = 260)
    png_domain = [180, 225, 50, 71]  # [W_lon, E_lon, S_lat, N_lat]

    # Enter directory name for saved .png file (using pathlib module)
    image_path = Path(output_directory)

    ################################################################################

    # Create VIIRS Smoke ADP map image file
    if file_list:
        plot_simple_viirs_adp_smoke(fs, file_list, png_domain, image_path)

    # Convert .png file to .tif file & save locally

    # ENTER USER SETTINGS

    # Enter domain for .tif file
    # Must use the same boundaries as domain used to make .png file!!!!
    # Note the order of entered lat/lon boundaries is different!
    # Enter longitude in 100 degrees (i.e., 100°W = -100)
    tif_domain = [-180, 72, -135, 50]  # [W_lon,  N_lat, E_lon, S_lat]

    # Enter directory name for saved .tif file (using pathlib module)
    tif_path = Path(output_directory)

    ################################################################################

    # Set full paths for .png and .tif files (as strings)
    # gdal takes file paths as strings
    image_file_path = (image_path / "viirs_adp.png").as_posix()
    tif_file_path = (tif_path / "viirs_adp.tif").as_posix()

    # Create geotiff
    print(f"Creating GeoTIFF viirs_adp.tif")
    create_geotiff(tif_file_path, image_file_path, tif_domain)


if __name__ == "__main__":
    # TODO Need a way to pass in the working directory to work with Prefect
    parser = argparse.ArgumentParser(
        description="Fetch VIIRS Smoke ADP data and create GeoTIFF."
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        required=True,
        help="Directory to output GeoTIFF to.",
    )
    args = parser.parse_args()

    main(args.out_dir)
