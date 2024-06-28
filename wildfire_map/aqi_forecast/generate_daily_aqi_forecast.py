from prefect import flow
from .aqi_forecast_tasks import *
from datetime import datetime, timedelta


def get_aqi_forecast_time():
    current_time = datetime.now()
    eight_am = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
    eight_pm = current_time.replace(hour=20, minute=0, second=0, microsecond=0)

    if eight_am <= current_time < eight_pm:
        adjusted_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        adjusted_time = current_time.replace(hour=12, minute=0, second=0, microsecond=0)

        # This will only happen if we happened to update the 12 PM update from the previous day
        # after 12 AM but before 8 AM on the next day. This shouldn't happen in the automated
        # workflow, but it's good to have it here just in case.
        if current_time < eight_am:
            adjusted_time -= timedelta(days=1)

    return adjusted_time.strftime("%Y%m%d%H")


@flow(log_prints=True)
def generate_daily_aqi_forecast(
    working_directory,
    script_name,
    netcdf_output_directory,
    tiff_output_directory,
):
    try:
        github_repo_dir = clone_github_repository(
            "update_frequency", f"{working_directory}"
        )

        install_conda_environment("aqi_forecasts", f"{github_repo_dir}/environment.yml")

        execute_local_script(
            f"{github_repo_dir}/{script_name}",
            netcdf_output_directory,
            tiff_output_directory,
        )
        return {"updated": get_aqi_forecast_time(), "succeeded": True}
    except Exception as e:
        return {
            "updated": get_aqi_forecast_time(),
            "succeeded": False,
            "error": str(e),
        }


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
