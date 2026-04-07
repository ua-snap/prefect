import xarray as xr
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.warp import calculate_default_transform, reproject, Resampling
import os


def netcdf_to_geotiff(input_netcdf, output_tiff, conda_env="hydrology"):
    dataset = xr.open_dataset(input_netcdf)

    variable = dataset["cdr_seaice_conc_monthly"].isel(time=0).values
    qa_flag = dataset["cdr_seaice_conc_monthly_qa_flag"].isel(time=0).values

    # Initialize output array - start with scaled concentration (0-100)
    rescaled_data = np.zeros_like(variable, dtype=np.float32)

    # Copy valid ice concentration values and scale to 0-100
    valid_mask = ~np.isnan(variable)
    rescaled_data[valid_mask] = variable[valid_mask] * 100

    # Apply landmask based on NaN values and QA flags
    # NaN with QA=0 are land pixels (not in ocean)
    # QA=16/80 pixels (bit 4) have conc=0 and should remain as ocean, not land
    nan_land_mask = np.isnan(variable) & (qa_flag == 0)
    rescaled_data[nan_land_mask] = 254  # Land

    # Mark remaining NaN values (non-land, outside data area) as nodata
    # This prevents gdalwarp from expanding land into these areas
    remaining_nan = np.isnan(variable) & (qa_flag != 0)
    rescaled_data[remaining_nan] = 255  # NoData

    # Convert to uint8 for output
    rescaled_data = rescaled_data.astype(np.uint8)

    # Pixel size is 25000 to match the 25 km grid
    pixel_size = 25000.0
    geotransform = (-3850000.0, pixel_size, 0.0, 5850000.0, 0.0, -pixel_size)
    # Define CRS using proj4 string to avoid PROJ database version issues
    source_crs = rasterio.CRS.from_proj4(
        "+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +x_0=0 +y_0=0 +a=6378273 +b=6356889.449 +units=m +no_defs"
    )
    target_crs = rasterio.CRS.from_proj4(
        "+proj=laea +lat_0=90 +lon_0=180 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    )

    with rasterio.MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff",
            height=rescaled_data.shape[0],
            width=rescaled_data.shape[1],
            count=1,
            dtype="uint8",
            crs=source_crs,
            transform=from_origin(
                geotransform[0], geotransform[3], pixel_size, pixel_size
            ),
        ) as src:
            src.write(rescaled_data, 1)

            # This reprojects the GeoTIFF data in memory to EPSG:3572
            transform, width, height = calculate_default_transform(
                src.crs, target_crs, src.width, src.height, *src.bounds
            )
            profile = src.profile.copy()
            profile.update(
                {
                    "crs": target_crs,
                    "transform": transform,
                    "width": width,
                    "height": height,
                    "compress": "lzw",
                }
            )

            with rasterio.open("temp.tif", "w", **profile) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest,
                )

    # Use gdalwarp to overwrite the file with the correct coordinates
    # We found that using rasterio for the warp resulted in incorrect coordinates
    # which caused the data to not properly ingest into the coverage.
    # Use nearest neighbor resampling to preserve categorical values (land=254, etc.)
    # Initialize destination to 0 (ocean) and use nodata=255 to prevent edge expansion
    os.system(
        f"gdalwarp -overwrite -q -multi -r near -ot Byte "
        "-srcnodata 255 -dstnodata 255 "
        "-wo INIT_DEST=0 "
        "-t_srs EPSG:3572 -te_srs EPSG:3572 "
        "-te -4862550.515 -4894840.007 4870398.248 4889334.803 "
        "-tr 17075.348707767432643 -17075.348707767432643 "
        f"'temp.tif' '{output_tiff}'"
    )

    # Verify the output file exists and print a message
    if os.path.exists(output_tiff):
        print(f"GeoTIFF {output_tiff} created successfully")
    else:
        print(f"Error: GeoTIFF {output_tiff} was not created successfully")
