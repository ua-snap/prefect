# WRF-Downscaled ERA5 4km Data Curation Flows

This directory contains Prefect flows for processing and archiving ERA5 downscaled data.

## Workflow Overview

### Processing Flow (`curate_era5_4km.py`)
- **Purpose**: Orchestrate ERA5 data processing on Chinook
- **Output**: Processed ERA5 data in specified directories

### Archival Flow (`archive_era5.py`)  
- **Purpose**: Archive completed data to Poseidon storage
- **Output**: Compressed archives on Poseidon

## Usage
Start a local Prefect server, something like:
```sh
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect server start
```
Once the server is running, serve the flow(s):
```sh
python curate_era5_4km.py
```
or
```sh
python archive_era5_simple.py
```
or, to serve both flows at once:
```sh
python curate_era5_4km.py & python archive_era5_simple.py &
```

## Reports
Both flows generate Prefect artifacts that can be examined in the Artifact tab of the Prefect UI.
