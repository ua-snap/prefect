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

import cartopy.feature as cfeature
from cartopy import crs as ccrs

import rasterio
from rasterio.transform import from_origin
from rasterio.crs import CRS
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy.ma as ma

from shapely.geometry import box
import geopandas as gpd


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


def process_viirs_adp_saai(ds):

    # Convert xarray DataArrays to NumPy masked arrays with correct dtype
    # Select "smoke present" (Smoke = 1)
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


def download_data(year, month, day):
    fs = s3fs.S3FileSystem(anon=True)
    products = fs.ls("noaa-nesdis-n21-pds")

    for product in products:
        print(product.split("/")[-1])

    bucket = "noaa-nesdis-n21-pds"
    product = "VIIRS-JRR-ADP"
    month = str(month).zfill(2)
    day = str(day).zfill(2)

    print(f"Year: {year}, Month: {month}, Day: {day}")

    data_path = (
        bucket + "/" + product + "/" + str(year) + "/" + str(month) + "/" + str(day)
    )

    files = fs.ls(data_path)

    print("Total number of files:", len(files), "\n")

    for file in files[:10]:
        print(file.split("/")[-1])

    # Enter search variables
    sat_name = "NOAA21"  #  Satellite name: 'SNPP', 'NOAA20', 'NOAA21'
    observation_date = f"{year}{month}{day}"  # 'YYYYMMDD'
    # Domain bounding box corners: °N lat > 0 > °S lat, °E lon > 0 > °W lon
    # Make the search domain a little larger than the map domain
    lower_left_lat_lon = "45,-180"  # southern-most lat, western-most lon (string)
    upper_right_lat_lon = "75,-130"  # northern-most lat, eastern-most lon (string)

    # Call the function
    start_times, end_times = find_jpss_observation_times(
        observation_date, lower_left_lat_lon, upper_right_lat_lon, sat_name
    )

    print("Overpass start times:", start_times)
    print("Overpass end times:", end_times)

    # Select NOAA-21   VIIRS ADP files for Alaska on June 27, 2024

    for start_time, end_time in zip(start_times, end_times):
        matches = [
            file
            for start_time, end_time in zip(start_times, end_times)
            for file in files
            if (
                file.split("/")[-1].split("_")[3][9:13] >= start_time
                and file.split("/")[-1].split("_")[3][9:13] <= end_time
            )
        ]

        # Print file names and approximate file sizes
        for match in matches:
            print(match.split("/")[-1])
            print("Approximate file size (MB):", round((fs.size(match) / 1.0e6), 2))
        for match in matches:
            # Check if file has already been downloaded
            if not (Path.cwd() / match.split("/")[-1]).exists():
                print("Now downloading", match.split("/")[-1])
                # Download file to Colab instance (cwd)
                fs.get(match, (Path.cwd() / match.split("/")[-1]).as_posix())


def png_main():

    # file_name = "JRR-ADP_v3r2_n21_s202406272126042_e202406272127289_c202406272211172.nc"

    # ds = xr.open_dataset((Path.cwd() / file_name), engine="netcdf4")
    # print(ds.Smoke.values)
    # print(ds.Smoke.encoding)

    # print(np.max(ds.SAAI.values), np.nanmin(ds.SAAI.where(ds.SAAI != -999.9).values))
    # download_data(2024, 6, 27)
    # download_data(2024, 6, 29)

    fig = plt.figure(figsize=(8, 10))

    # Set map projection using cartopy
    # Set central_longitude=180 for Alaska
    ax = plt.axes(projection=ccrs.Mercator(central_longitude=180))

    # Set geographic domain of map: [W_lon, E_lon, S_lat, N_lat]
    # Use 360 degrees for longitude (i.e., 100 °W = 260)
    # °N latitude > 0 > °S latitude
    ax.set_extent([180, 225, 50, 72], crs=ccrs.PlateCarree())

    # Add coastlines & borders, shade land & water polygons
    ax.add_feature(cfeature.COASTLINE, linewidth=0.75)
    ax.add_feature(cfeature.BORDERS, linewidth=0.75)
    ax.add_feature(cfeature.LAKES, facecolor="lightgrey")
    ax.add_feature(cfeature.LAND, facecolor="grey")
    ax.add_feature(cfeature.OCEAN, facecolor="lightgrey")

    # Set colormaps & normalization for plotting data
    norm1 = mpl.colors.Normalize(vmin=0, vmax=2)
    cmap1 = plt.get_cmap("PuRd")

    # Collect all of the downloaded VIIRS ADP granule files
    file_list = sorted(Path.cwd().glob("JRR-ADP*.nc"))

    # Loop through files
    for file in file_list:
        # Print the file name (to monitor progress)
        print("Now processing", file.name)
        # Open file using xarray (automatically closes file when done)
        with xr.open_dataset(file, engine="netcdf4") as ds:

            # Process VIIRS ADP SAAI
            saai_smoke = process_viirs_adp_saai(ds)

            # Interpolate missing latitude & longitude values
            latitude_interpolated = interpolate_missing_pixels(ds.Latitude)
            longitude_interpolated = interpolate_missing_pixels(ds.Longitude)

            # Plot data
            ax.pcolormesh(
                longitude_interpolated,
                latitude_interpolated,
                saai_smoke,
                cmap=cmap1,
                norm=norm1,
                transform=ccrs.PlateCarree(),
            )

        # Add smoke intensity colorbar
        cb1_ax = ax.inset_axes([0.225, -0.05, 0.25, 0.03])
        cb1 = mpl.colorbar.ColorbarBase(
            cb1_ax,
            cmap=cmap1,
            norm=norm1,
            ticks=[0.25, 1.75],
            orientation="horizontal",
        )
        cb1.set_label(label="Smoke", size=10, weight="bold")
        cb1.ax.set_xticklabels(["Thin", "Thick"])
        cb1.ax.tick_params(which="major", length=0, labelsize=8)

        # Save image file
        save_name = (
            file_list[-1].name.split("_")[2]
            + "_viirs_adp_saai_"
            + file_list[-1].name.split("_")[3][1:9]
        )
        fig.savefig(save_name, dpi=300, bbox_inches="tight")

        # Close plot
        plt.close()


def geotiff_main():
    # Collect all of the downloaded VIIRS ADP granule files
    file_list = sorted(Path.cwd().glob("JRR-ADP*.nc"))

    for file in file_list:
        print("Now processing", file.name)
        with xr.open_dataset(file, engine="netcdf4") as ds:
            # Process VIIRS ADP SAAI
            saai_smoke = process_viirs_adp_saai(ds)

            # Interpolate missing lat/lon
            latitude_interpolated = interpolate_missing_pixels(ds.Latitude)
            longitude_interpolated = interpolate_missing_pixels(ds.Longitude)

            # Convert masked array to regular array with NaNs
            data = saai_smoke.filled(np.nan)

            # Convert longitudes from 0-360 to -180 to 180 range
            longitude_interpolated = np.where(
                longitude_interpolated > 180,
                longitude_interpolated - 360,
                longitude_interpolated,
            )

            # Ensure data and coordinates are properly oriented
            # Sort by latitude (descending) and longitude (ascending)
            lat_sorted_idx = np.argsort(latitude_interpolated[:, 0])[::-1]
            lon_sorted_idx = np.argsort(longitude_interpolated[0, :])

            latitude_interpolated = latitude_interpolated[lat_sorted_idx][
                :, lon_sorted_idx
            ]
            longitude_interpolated = longitude_interpolated[lat_sorted_idx][
                :, lon_sorted_idx
            ]
            data = data[lat_sorted_idx][:, lon_sorted_idx]

            # Calculate pixel size from sorted coordinates
            pixel_height = abs(
                latitude_interpolated[1, 0] - latitude_interpolated[0, 0]
            )
            pixel_width = abs(
                longitude_interpolated[0, 1] - longitude_interpolated[0, 0]
            )

            # Get top-left coordinates (maximum latitude, minimum longitude)
            top_left_lat = np.max(latitude_interpolated)
            top_left_lon = np.min(longitude_interpolated)

            transform = from_origin(
                top_left_lon, top_left_lat, pixel_width, pixel_height
            )

            # Define metadata
            new_dataset = {
                "driver": "GTiff",
                "height": data.shape[0],
                "width": data.shape[1],
                "count": 1,
                "dtype": "float32",
                "crs": CRS.from_epsg(4326),
                "transform": transform,
                "nodata": np.nan,
            }

            # Unique filename
            start_timestamp = file.name.split("_")[3][1:]
            save_name = (
                file.name.split("_")[2] + "_viirs_adp_saai_" + start_timestamp + ".tif"
            )

            with rasterio.open(save_name, "w", **new_dataset) as dst:
                dst.write(data, 1)

            print(f"Saved GeoTIFF: {save_name}")

    # Merge all GeoTIFFs into one
    combine_geotiffs()


def combine_geotiffs(output_name="viirs_smoke.tif"):
    tiff_files = sorted(Path.cwd().glob("*_viirs_adp_saai_*.tif"))
    src_files_to_mosaic = [rasterio.open(fp) for fp in tiff_files]

    # Merge using the highest smoke value at overlapping pixels
    mosaic, out_transform = merge(src_files_to_mosaic, method="max")

    out_meta = src_files_to_mosaic[0].meta.copy()
    out_meta.update(
        {
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_transform,
            "count": 1,
        }
    )

    with rasterio.open(output_name, "w", **out_meta) as dest:
        dest.write(mosaic[0], 1)

    print(f"Saved merged single-band GeoTIFF: {output_name}")


def reproject_to_alaska(input_path, output_path):
    dst_crs = "EPSG:3338"  # Alaska Albers Equal Area

    with rasterio.open(input_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update(
            {"crs": dst_crs, "transform": transform, "width": width, "height": height}
        )

        with rasterio.open(output_path, "w", **kwargs) as dst:
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=dst_crs,
                resampling=Resampling.nearest,
            )

    print(f"Reprojected GeoTIFF saved as: {output_path}")


if __name__ == "__main__":
    download_data(2024, 6, 27)
    # png_main()
    # tif_main()
    geotiff_main()
    # reproject_to_alaska("viirs_smoke.tif", "viirs_smoke_alaska.tif")
