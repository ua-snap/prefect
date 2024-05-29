from prefect import flow
from .aqi_forecast_tasks import *


@flow(log_prints=True)
def generate_daily_aqi_forecast(
    working_directory,
    script_name,
    netcdf_output_directory,
    tiff_output_directory,
):
    github_repo_dir = clone_github_repository("main", f"{working_directory}")

    install_conda_environment("aqi_forecasts", f"{github_repo_dir}/environment.yml")

    execute_local_script(
        f"{github_repo_dir}/{script_name}",
        netcdf_output_directory,
        tiff_output_directory,
    )


if __name__ == "__main__":
    generate_daily_aqi_forecast.serve(
        name="Generate Daily AQI Forecast for Wildfire Map",
        tags=["aqi_forecast", "wildfire_map"],
        parameters={
            "working_directory": "/usr/local/prefect/wildfire_map/aqi_forecast",
            "script_name": "A_B_combined.py",
            "netcdf_output_directory": "/usr/local/prefect/wildfire_map/aqi_forecast/netcdf_output/",
            "tiff_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers",
        },
    )
