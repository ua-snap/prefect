## Executing ARDAC notebooks via Prefect & Papermill

The intention of these scripts is to execute a production run of all of the notebooks in a given branch of the `ardac-toolbox` repo using `prefect`. This way we can ensure that all notebooks are actually executed successfully, and are staged in a specific location for final QC before publishing to the ARDAC website.

To accomplish this, we create a task in the `prefect` flow that will execute the notebooks from the command line using `papermill`, e.g.:


```
papermill path/to/development/notebook.ipynb path/to/production/notebook.ipynb -r argument "argument raw string"
```


The first argument is this notebook's location, which can be constructed using the `{working_directory}` parameter of the flow run (ie, the notebook's location within the downloaded repo directory). The second argument is the desired notebook output location, which can also be constructed using the `{working_directory}` parameter of the flow run.

The remaining arguments are raw strings (`-r`) of additional arguments passed to the notebook. These arguments can be constructed from flow run parameters, provided explicitly, or not provided at all. If no arguments are provided the notebook will simply execute "as is", which is probably the most likely use case for this project.