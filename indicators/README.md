# Computing CMIP6 Indicators via Prefect

After following the setup instructions from this repo's main [README](https://github.com/ua-snap/prefect/blob/main/README.md), start the Prefect server and navigate to the `prefect/indicators` directory. Start the indicator workflow deployment like so:

```
python generate_indicators.py
```

Open the Prefect UI in your browser, and click `Run > Quick Run` at the top right. Fill in the parameters:

- `{ssh_username}`: your username on Chinook
- `{ssh_private_key_path}`: path to your ssh key on your local machine running the Prefect server
- `{branch_name}`: the branch of the `cmip6-utils` repo that you would like to use in the deployment
- `{working_directory}`: scratch directory on Chinook where output files will be written
- `{indicators}`: a list of indicators formatted as a string and separated by whitespace (e.g. "rx1day su dw ftc")
- `{models}`: a list of models formatted as a string and separated by whitespace (e.g. "CESM2 GFDL-ESM4 TaiESM1")
- `{scenarios}`: a list of models formatted as a string and separated by whitespace (e.g. "historical ssp126 ssp245 ssp370 ssp585")
- `{input_dir}`: path to regridded CMIP6 data (should be "/import/beegfs/CMIP6/arctic-cmip6/regrid")


To see all the possible choices for models, scenarios, and indicators, check out the various lookup tables and configuration files in the [cmip6-utils](https://github.com/ua-snap/cmip6-utils/tree/main) repo. Alternatively, you can use the "all" string in any/all of the `{models}`, `{scenarios}`, and `{indicators}` parameters to run all possible choices.
