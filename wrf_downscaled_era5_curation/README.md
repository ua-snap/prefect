# WRF-Downscaled ERA5 4km Flows

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

When developing these flows, Prefect yelled about not being able to cache some objects (like SSH connections, which makes sense) - and while these warnings didn't prohibit successful execution, they were annoying and made it hard for me to read the logs so I squashed them like this, and you will likely want to do the same:

```sh
 export PREFECT_TASKS_DEFAULT_NO_CACHE=true
 ```

Once the server is running, serve the flow(s):
```sh
python curate_era5_4km.py
```
or
```sh
python archive_era5.py
```
or, to serve both flows at once:
```sh
python curate_era5_4km.py & python archive_era5.py &
```

### SSH Agent Forwarding
The archival flow requires SSH access to both Chinook and Poseidon - what I've been doing on my laptop is to ensure that the SSH agent is running and loaded with my key:
```sh
ssh-add ~/.ssh/id_rsa
```

## Reports
Both flows generate Prefect artifact objects that can be examined in the "Artifact" tab of the Prefect UI.
