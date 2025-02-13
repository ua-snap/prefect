# SNAP Prefect Workflows

## Download Prefect on your local system

`pip install -U prefect paramiko`

This repo now includes a [`pyproject.toml`](pyproject.toml) file to allow this repo to be "installed" so that code can be shared across the different directories containing flows. This has only been implemented in the regridding flows so far. 

To install, activate the environment you use for prefect, and run:

```
python -m pip install .
```

## Use the production Prefect server for running real tasks

To set your Terminal window to use the production Prefect server, run the following:

`prefect config set PREFECT_API_URL=https://prefect.earthmaps.io/api`

Now when you trigger a workflow run, you will use the production Prefect server to schedule the task. This also allows for the run to be logged on a shared resource for review by the entire team.

## Run a local Prefect server to connect your flows

```
prefect server start

 ___ ___ ___ ___ ___ ___ _____
| _ \ _ \ __| __| __/ __|_   _|
|  _/   / _|| _|| _| (__  | |
|_| |_|_\___|_| |___\___| |_|

Configure Prefect to communicate with the server with:

    prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

View the API reference documentation at http://127.0.0.1:4200/docs

Check out the dashboard at http://127.0.0.1:4200
```

## In a different terminal window, set the server URL to the locally created Prefect server

`prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`

## Start a workflow deployment

```
$ python gipl_ingest.py

╭────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ Your flow 'ingest-flow' is being served and polling for scheduled runs!                            │
│                                                                                                    │
│ To trigger a run for this flow, use the following command:                                         │
│                                                                                                    │
│         $ prefect deployment run 'ingest-flow/gipl-ingest'                                         │
│                                                                                                    │
│ You can also run your flow via the Prefect UI:                                                     │
│ http://127.0.0.1:4200/deployments/deployment/91174ebd-4715-4399-953e-42efbb889d94                  │
│                                                                                                    │
╰────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

Go to localhost:4200 and click on Deployments, the name of the deployment you want, and hit Run at the top right of the screen. Adjust the parameters to match your environment for your username, your SSH key, etc.
