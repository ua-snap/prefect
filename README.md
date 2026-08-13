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


## Serving a scheduled flow run on production

Setting `PREFECT_API_URL` only tells Prefect which server to talk to. For a recurring schedule (e.g. our flows with deployed with `.serve(..., cron=...))` the long-running process must stay online on the production host so it can claim scheduled runs. If that process is down, runs appear 'Late" in the Prefect UI.

We use PM2 (usually as the `snapdata` user) on the relevant host (e.g. chinook04) to keep the serve process alive.

PM2 is a daemon process manager.

To use PM2 to start the Prefect process:

- Verify PM2 is available: `pm2 --version`
- Ensure the `ua-snap/prefect` repo is available on that host at a stable path
- Ensure there is an environment available on that host that can import Prefect and the flow's dependencies

Then...

1. Create a PM2 configuration file, or add to an existing `pm2.config.js`

Drought Indicators Example
```sh
module.exports = {
    apps: [
        {
            name: 'drought_indicators', # descriptive name, how you want it to show up in `pm2 list`
            script: 'prefect/drought/run_drought_pipeline.py', # likely a symlink
            interpreter: '/import/home/snapdata/miniconda3/envs/prefect/bin/python' # must be an absolute path
        }
    ],
    hooks: {
        pre_start: {
            cmd: 'prefect config set PREFECT_API_URL=https://prefect.earthmaps.io/api',
            blocking: true # don't start the PM2 application process unless the above command exits zero
        }
    }
}
```

Remember, it is either the cron statement in the flow's .serve(...), or the schedule added in the Prefect UI post-deployment that creates the schedule. PM2 only keeps the process running.

2. Start and persist the process

```sh
pm2 start pm2.config.js
pm2 list
```
You should see your process running!

3. Verify 
In the Prefect UI (https://prefect.earthmaps.io), the deployment should show as served / polling.
Scheduled runs should move Scheduled → Pending → Running. A persistently "Late" status means something is wrong. Most likely the PM2 process is dead.

4. Set the process up to spawn again upon reboot

```sh
pm2 save
```

In the event of a server restart, which will happen at some point, your process will fire back up on reboot.

### Host Selection
It's worth choosing a host that is relevant to your flow. For example, the Wildfire related flows are kept alive by PM2 processes on GeoServer. We don't need tons of processing power for those flows, and the outcome of those flows are layers on GeoServer -- so it makes sense to host the PM2 processes there. But for drought indicators, we need HPC resources -- so the flow serving process is kept alive on Chinook. By considering the host(s) in play we can minimize the amount of SSH-hopping via `paramiko` and reduce the overall code surface area in each flow. However, it does mean that you'll likely need to develop on the remote host instead of on your local machine.
