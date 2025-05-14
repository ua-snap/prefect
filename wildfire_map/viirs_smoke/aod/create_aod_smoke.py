"""
The script is designed to be used with Prefect to create a daily GeoTIFF snapshot of VIIRS
Aerosol Optical Depth (AOD) data over Alaska.

Outputs:
- Single-band GeoTIFF of Aerosol Optical Depth
"""

import datetime
from datetime import date
import argparse

import s3fs
import requests
import pandas as pd
import xarray as xr
import numpy as np
from scipy.interpolate import griddata
from osgeo import gdal, osr


def find_jpss_observation_times(
    observation_date, lower_left_lat_lon, upper_right_lat_lon, sat_name
):
    """Find observation times for JPSS satellites within a specified bounding box. Uses UWisc OrbNav API.
    Args:
        observation_date (str): Date in YYYYMMDD format.
        lower_left_lat_lon (str): Lower left corner of bounding box in "lat,lon" format.
        upper_right_lat_lon (str): Upper right corner of bounding box in "lat,lon" format.
        sat_name (str): Satellite name (e.g., "SNPP", "NOAA20", "NOAA21").
    Returns:
        start_times (list): List of start times in HHMM format.
        end_times (list): List of end times in HHMM format.
    """
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

    enter_list = combined["enter_datetime"].tolist()
    leave_list = combined["leave_datetime"].tolist()
    # Remove the colon from the list of enter/leave times (strings)
    # Need 'HHMM' format for use with satellite data file names
    start_times = [time[11:16].replace(":", "") for time in enter_list]
    end_times = [time[11:16].replace(":", "") for time in leave_list]

    return start_times, end_times


def query_nodd_viirs_aod(fs, observation_date, sat_name, start_times, end_times):
    """Query AWS NODD for available VIIRS operational AOD EDR data files.
    Args:
        fs (s3fs.S3FileSystem): S3 filesystem object.
        observation_date (str): Date in YYYYMMDD format.
        sat_name (str): Satellite name (e.g., "SNPP", "NOAA20", "NOAA21").
        start_times (list): List of start times in HHMM format.
        end_times (list): List of end times in HHMM format.
    Returns:
        nodd_file_list (list): List of available NODD files.
    """
    # Define terms in directory paths on AWS NODD
    year = observation_date[:4]
    month = observation_date[4:6]
    day = observation_date[6:]
    # Make dictionary for JPSS satellite/ADP directory paths on AWS NODD
    # Keys for JPSS satellites
    keys = ["SNPP", "NOAA20", "NOAA21"]
    # Values for operational VIIRS AOP EDR directory paths
    values = [
        "noaa-nesdis-snpp-pds/VIIRS-JRR-AOD/",
        "noaa-nesdis-n20-pds/VIIRS-JRR-AOD/",
        "noaa-nesdis-n21-pds/VIIRS-JRR-AOD/",
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


def process_viirs_aod(ds, aod_quality):
    """Process VIIRS AOD and screen data with quality flag.
    Args:
        ds (xarray.Dataset): Xarray dataset containing VIIRS ADP data.
        aod_quality (str): Quality flag for AOD data ("high", "top2", "all").
    Returns:
        ds.AOD550 (xarray.DataArray): Processed AOD data with quality screening.
    """
    # Set AOD data quality
    if aod_quality == "high":
        set_quality = ds.QCAll == 0
    elif aod_quality == "top2":
        set_quality = ds.QCAll <= 1
    elif aod_quality == "all":
        set_quality = ds.QCAll <= 2

    return ds.AOD550.where(set_quality)


def interpolate_missing_pixels(da):
    """Interpolate missing pixels in a DataArray using nearest neighbor method.
    Args:
        da (xarray.DataArray): DataArray with missing values to interpolate.
    Returns:
        da_interpolated (numpy.ndarray): Interpolated DataArray.
    """
    height, width = da.values.shape

    # Create 2D arrays with dimensions of x (width) and y (height)
    xx, yy = np.meshgrid(np.arange(width), np.arange(height))
    mask = np.isnan(da)
    # 1D arrays of known (non-masked) x, y indices (locations)
    # np.logical_not() flips the boolean array
    known_x = xx[np.logical_not(mask)]
    known_y = yy[np.logical_not(mask)]
    # 1D arrays of missing (masked) x, y indices (locations)
    missing_x = xx[mask]
    missing_y = yy[mask]

    # 1D array of known (non-masked) DataArray values
    known_v = da.to_masked_array().compressed()

    # Interpolate missing DataArray values (returns 1D array)
    interpolated_values = griddata(
        (known_x, known_y), known_v, (missing_x, missing_y), method="nearest"
    )
    # Assign interpolated values to indexed DataArray (replace NaNs)
    da_interpolated = da.values.copy()
    da_interpolated[missing_y, missing_x] = interpolated_values

    return da_interpolated


def normalize_longitudes(longitudes):
    """Convert longitudes to -180 to 180 range."""
    return ((longitudes + 180) % 360) - 180


def create_empty_grid(domain, resolution=0.02):
    """Create empty grid array and geotransform for a given domain

    Parameters:
    -----------
    domain : list
        Geographic domain [west_lon, east_lon, south_lat, north_lat]
    resolution : float
        Grid resolution in degrees. This is going to determine the ultimate spatial resolution.

    Returns:
    --------
    tuple
        (grid_array, geotransform)
    """
    west_lon, east_lon, south_lat, north_lat = domain

    # Calculate grid dimensions
    x_size = int((east_lon - west_lon) / resolution)
    y_size = int((north_lat - south_lat) / resolution)

    # Create empty grid array initialized with NaN
    grid_array = np.full((y_size, x_size), np.nan, dtype=np.float32)

    # Define geotransform: (top-left x, w-e pixel resolution, 0, top-left y, 0, n-s pixel resolution (negative))
    geotransform = (west_lon, resolution, 0, north_lat, 0, -resolution)

    return grid_array, geotransform, (x_size, y_size)


# Project VIIRS data onto the grid
def project_data_to_grid(grid_array, geotransform, lon, lat, data, grid_size):
    """Project data from VIIRS swath coordinates to a regular grid

    Parameters:
    -----------
    grid_array : numpy.ndarray
        Target grid array
    geotransform : tuple
        GDAL geotransform tuple
    lon : numpy.ndarray
        2D array of longitudes
    lat : numpy.ndarray
        2D array of latitudes
    data : numpy.ndarray
        2D array of data values
    grid_size : tuple
        (x_size, y_size) of grid

    Returns:
    --------
    numpy.ndarray
        Updated grid array with new data
    """
    # Unpack geotransform parameters
    x_origin, pixel_width, _, y_origin, _, pixel_height = geotransform
    x_size, y_size = grid_size

    data = data.values

    # Get valid data points (not masked)
    if hasattr(data, "mask"):
        valid_mask = ~data.mask
        lats = lat[valid_mask]
        lons = lon[valid_mask]
        values = data.data[valid_mask]
    else:
        valid_mask = ~np.isnan(data)
        lats = lat[valid_mask]
        lons = lon[valid_mask]
        values = data[valid_mask]

    # Skip if no valid data
    if len(lats) == 0:
        return grid_array

    # Create a copy of the grid array to update
    updated_grid = grid_array.copy()
    value_counts = np.zeros_like(grid_array, dtype=np.int32)

    # For each valid point, compute its position in the grid
    for i in range(len(lats)):
        # Convert lat/lon to grid row/col
        col = int((lons[i] - x_origin) / pixel_width)
        row = int((y_origin - lats[i]) / -pixel_height)

        # Check if within grid bounds
        if (0 <= row < y_size) and (0 <= col < x_size):
            # If cell is empty, just set the value
            if np.isnan(updated_grid[row, col]):
                updated_grid[row, col] = values[i]
                value_counts[row, col] = 1
            else:
                # Otherwise, compute running average
                current_count = value_counts[row, col]
                updated_grid[row, col] = (
                    updated_grid[row, col] * current_count + values[i]
                ) / (current_count + 1)
                value_counts[row, col] += 1

    return updated_grid


# Create GeoTIFF directly from raster array
def create_geotiff_from_array(output_file, array, geotransform, nodata_value=None):
    """Create a GeoTIFF file directly from an array

    Parameters:
    -----------
    output_file : str
        Path to output GeoTIFF file
    array : numpy.ndarray
        2D array of data
    geotransform : tuple
        GDAL geotransform tuple
    nodata_value : float, optional
        No data value
    """
    # Get array dimensions
    y_size, x_size = array.shape

    # Create output driver
    driver = gdal.GetDriverByName("GTiff")

    # Create output dataset
    dataset = driver.Create(
        output_file,
        x_size,
        y_size,
        1,  # Number of bands
        gdal.GDT_Float32,
        options=["COMPRESS=DEFLATE", "TILED=YES"],  # Compression options
    )

    # Set geotransform and projection
    dataset.SetGeoTransform(geotransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dataset.SetProjection(srs.ExportToWkt())

    # Write data
    band = dataset.GetRasterBand(1)
    if nodata_value is not None:
        band.SetNoDataValue(nodata_value)
    band.WriteArray(array)

    # Close dataset
    dataset = None


def main(output_dir):

    # Enter search variables for VIIRS AOD EDR data files on AWS NODD
    # Satellite name: 'SNPP', 'NOAA20', 'NOAA21'
    sat_name = "NOAA21"

    # for production
    observation_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    )

    # below block for testing
    ##
    # from pathlib import Path
    # observation_date = "20240627"
    # output_dir = Path.cwd() / "output"
    # output_dir.mkdir(exist_ok=True)
    ##

    # Connect to AWS S3
    fs = s3fs.S3FileSystem(anon=True)

    # Find the overpass start/end times for AK, east of Antimeridian
    east_start_times, east_end_times = find_jpss_observation_times(
        observation_date, "45,-180", "75,-130", sat_name
    )
    # Query AWS NODD for available files for AK, ast of Antimeridian
    east_file_list = query_nodd_viirs_aod(
        fs, observation_date, sat_name, east_start_times, east_end_times
    )
    # Find "tomorrow's" 8-digit date as a string
    # Date for VIIRS granules falling west of Antimeridian is "tomorrow" in UTC
    obs_date = datetime.datetime.strptime(observation_date, "%Y%m%d").date()
    tomorrow = (obs_date + datetime.timedelta(days=1)).strftime("%Y%m%d")
    # Find the overpass start/end times for AK, west of Antimeridian
    west_start_times, west_end_times = find_jpss_observation_times(
        tomorrow, "45,170", "75,180", sat_name
    )
    # Query AWS NODD for available files for AK, west of Antimeridian
    west_file_list = query_nodd_viirs_aod(
        fs, tomorrow, sat_name, west_start_times, west_end_times
    )

    file_list = east_file_list + west_file_list
    print(f"Found {len(file_list)} files.")
    if not file_list:
        print("No files found. Exiting.")
        return

    # Alaska domain for GeoTIFF in -180/180 longitude range
    # Convert from 0-360 to -180/180 for the western side
    domain = [-180, -130, 50, 72]  # [W_lon, E_lon, S_lat, N_lat]

    print("Creating empty grid for Alaska domain...")
    grid_array, geotransform, grid_size = create_empty_grid(domain)

    # Process each file and project data onto grid
    print("Processing files and projecting data onto grid...")
    for idx, file in enumerate(file_list):
        print(f"Processing {file.split('/')[-1]}")
        with fs.open(file, mode="rb") as remote_file:
            with xr.open_dataset(remote_file, engine="h5netcdf") as ds:
                # Process AOD data
                aod = process_viirs_aod(ds, "top2")

                # Interpolate missing lat/lon
                lat = interpolate_missing_pixels(ds.Latitude)
                lon = interpolate_missing_pixels(ds.Longitude)

                # Normalize longitudes to -180/180 range
                lon = normalize_longitudes(lon)

                # Project data onto grid
                grid_array = project_data_to_grid(
                    grid_array, geotransform, lon, lat, aod, grid_size
                )

    # retain for testing and use for output name
    # save_name = f"{sat_name}_viirs_aerosol_optical_depth_{observation_date}"

    tif_path = output_dir / f"viirs_aod.tif"
    print(f"Creating GeoTIFF: {tif_path}")
    create_geotiff_from_array(tif_path, grid_array, geotransform, nodata_value=np.nan)

    print("Processing complete.")


if __name__ == "__main__":
    # TODO Need a way to pass in the working directory to work with Prefect
    parser = argparse.ArgumentParser(
        description="Fetch VIIRS Aerosol Optical Depth data and create GeoTIFF."
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        required=True,
        help="Directory to output GeoTIFF to.",
    )
    args = parser.parse_args()

    main(args.out_dir)
