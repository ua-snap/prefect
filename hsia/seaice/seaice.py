import xarray as xr
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.warp import calculate_default_transform, reproject, Resampling


def netcdf_to_geotiff(input_netcdf, output_tiff):
    dataset = xr.open_dataset(input_netcdf)

    variable = dataset["F17_ICECON"].isel(time=0).values

    # Need to scale the data to match the concentration values for the TIFFs
    rescaled_data = variable * 100

    rescaled_data[np.isclose(rescaled_data, 100.4)] = 251  # Circular mask
    rescaled_data[np.isclose(rescaled_data, 100.8)] = 252  # Unused
    rescaled_data[np.isclose(rescaled_data, 101.2)] = 253  # Coastlines
    rescaled_data[np.isclose(rescaled_data, 101.6)] = 254  # Land mask
    rescaled_data[np.isclose(rescaled_data, 102.0)] = 255  # Missing data

    # Matches the original TIFFs for output consistency
    rescaled_data = rescaled_data.astype(np.uint8)

    # Pixel size is 25000 to match the 25 km grid
    pixel_size = 25000.0
    geotransform = (-3850000.0, pixel_size, 0.0, 5850000.0, 0.0, -pixel_size)
    source_crs = "+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +a=6378273 +b=6356889.449 +units=m +no_defs"
    target_crs = "EPSG:3572"

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

            with rasterio.open(output_tiff, "w", **profile) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest,
                )

    print(f"GeoTIFF {output_tiff} created successfully.")
