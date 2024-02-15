## Executing ARDAC notebooks via Prefect & Papermill

The intention of these scripts is to execute a production run of all of the notebooks in a given branch of the `ardac-toolbox` repo using `prefect`. This way we can ensure that all notebooks are actually executed successfully, and are staged in a specific location for final QC before publishing to the ARDAC website.

To accomplish this, we create a task in the `prefect` flow that will execute the notebooks from the command line using `papermill`, e.g.:


```
papermill path/to/development/notebook.ipynb path/to/production/notebook.ipynb -r argument "argument raw string"
```


The first argument is a notebook's location, which can be constructed from the `{working_directory}` parameter in the flow run (e.g. `{working_directory}/ardac-toolbox/notebooks/...`). The second argument is the desired notebook output location, which can be also be constructed using the `{working_directory}` parameter of the flow run.

The remaining arguments are raw strings (`-r`) of additional arguments passed to the notebook. These arguments can be constructed from flow run parameters, provided explicitly, or not provided at all. If no arguments are provided the notebook will simply execute "as is", which is probably the most likely use case for this project.

This is still experimental and has not been tested!

A lot of the `prefect` code here is copied verbatim from the indicators work, so any major changes there should probably be incorporated here as well. This scripts does not make use of slurm scrips to run the notebooks, but that could be incorporated in the future to improve processing time.

**Note: Before running this, your branch in the `ardac-toolbox` repo will need a `conda_init.sh` script!**