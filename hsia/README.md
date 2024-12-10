# Sea Ice GeoTIFF Generation

This repository contains scripts to generate monthly sea ice concentration GeoTIFFs from NetCDF files. The scripts automate tasks like downloading data from the National Snow and Ice Data Center (NSIDC), processing it into GeoTIFFs, and archiving the results for ingestion into Rasdaman.

---

## Table of Contents

1. [Overview](#overview)
2. [Scripts Description](#scripts-description)
   - [generate_annual_hsia_rasdaman_data.py](#generate_annual_hsia_rasdaman_datapy)
   - [hsia_tasks.py](#hsia_taskspy)
   - [seaice/seaice.py](#seaiceseaicepy)
3. [Installation](#installation)
4. [How to Run the Scripts](#how-to-run-the-scripts)
   - [Running the Generate Script](#running-the-generate-script)
5. [Dependencies](#dependencies)

---

## Overview

These scripts handle the end-to-end process of generating GeoTIFF files from NSIDC sea ice concentration NetCDF data. The key steps include:

1. Downloading monthly NetCDF data for specified years.
2. Converting NetCDF data to GeoTIFF format.
3. Archiving the processed GeoTIFFs.
4. Copying the archived GeoTIFFs to long-term storage.

---

## Scripts Description

### 1. `generate_annual_hsia_rasdaman_data.py`

This is the main script orchestrating the workflow to generate annual sea ice GeoTIFFs. It uses Prefect to define and run the workflow.

**Key Parameters:**

- `years`: Year or list of years to process.
- `home_directory`: Directory for the home path (used for writing our team's credentials into a .netrc file for downloading the NSIDC data).
- `working_directory`: Temporary working directory for processing data.
- `source_tar_file`: Path to the source tar file containing previous years' sea ice concentration GeoTIFFs.
- `tif_directory`: Directory to store the generated GeoTIFFs.

**Flow Steps:**

1. Check for NSIDC credentials.
2. Check if the NFS mount is available.
3. Copy data from the NFS mount.
4. Untar the data.
5. Download new NSIDC data.
6. Generate annual sea ice GeoTIFFs.
7. Archive the GeoTIFFs into a Gzipped tar file.
8. Copy the tar file to the storage server.

### 2. `hsia_tasks.py`

This script contains Prefect tasks used in the main flow. It handles various operations such as:

- Checking for NFS mounts.
- Copying data from the NFS mount.
- Downloading new NSIDC data for the specified year.
- Converting NetCDF files to GeoTIFFs.
- Archiving the GeoTIFFs.
- Copying the tar file to a storage server.

### 3. `seaice/seaice.py`

This script handles the conversion of NSIDC sea ice concentration NetCDF files to GeoTIFF format.

**Key Function:**

- `netcdf_to_geotiff(input_netcdf, output_tiff)`:
  - Reads the NetCDF file and rescales the data.
  - Converts the data to GeoTIFF format.
  - Reprojects the data to EPSG:3572.
  - Uses `gdalwarp` for accurate georeferencing.

---

## Installation

### Note

If using the snapdata user on our production Rasdaman server, all of this has already been done, so all that needs to be done is to run:

```bash
source /opt/miniconda3/bin/activate
python prefect/hsia/generate_annual_hsia_rasdaman_data.py
```

The start-up portion of the script will tell you how to run the workflow on the server or you can go to the production Prefect server.

### Prerequisites

- Python 3.8+
- Prefect
- xarray
- NumPy
- rasterio
- GDAL
- netrc

### Install Dependencies

Run the following command to install the necessary Python packages:

```bash
pip install prefect xarray numpy rasterio netrc gdal
```

### Setup NSIDC Credentials

Ensure you have NSIDC credentials saved as a Prefect Secret named `nsidc-netrc-credentials`.

---

## How to Run the Scripts

### Running the Generate Script

To run the main script and generate annual sea ice GeoTIFFs, use the following command:

```bash
python generate_annual_hsia_rasdaman_data.py
```

**Example Run with Parameters:**

```python
hsia.serve(
    name="Update Annual Sea Ice GeoTIFFs for ingest into Rasdaman",
    tags=["HSIA", "Sea Ice", "Create New Data"],
    parameters={
        "years": 2024,
        "home_directory": "/home/snapdata/",
        "working_directory": "/opt/rasdaman/user_data/snapdata/hsia_updates/",
        "source_tar_file": "/workspace/Shared/Tech_Projects/Sea_Ice_Atlas/final_products/rasdaman_hsia_arctic_production_tifs.tgz",
        "tif_directory": "/opt/rasdaman/user_data/snapdata/hsia_updates/rasdaman_hsia_arctic_production_tifs",
    },
)
```

### Expected Results

- Downloaded NetCDF data for the specified year(s).
- GeoTIFFs in the specified `tif_directory`.
- Archived tar file copied to the storage server.

---

## Dependencies

- **Python Libraries:** Prefect, xarray, NumPy, rasterio
- **Tools:** GDAL, wget, scp

---

## Notes

- Ensure NFS mounts are available before running the scripts.
- The scripts assume a working directory with sufficient storage for processing large datasets. It is suggested to use /opt/rasdaman/user_data/<`username`>
