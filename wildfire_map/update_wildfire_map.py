from prefect import flow
from fire_layers.current_fire_layers import current_fire_layers
from smokey_bear.smokey_bear_layer import smokey_bear_layer
from smokey_bear.snow_cover_layer import snow_cover_layer
from aqi_forecast.generate_daily_aqi_forecast import generate_daily_aqi_forecast
from purple_air.get_daily_purple_air import purple_air
from datetime import datetime


@flow(log_prints=True)
def update_wildfire_layers(
    debug,
    home_directory,
    working_directory,
    aqi_forecast_netcdf_path,
    shapefile_output_directory,
    purple_air_shapefile_output_directory,
):
    status = {"updated": datetime.now().strftime("%Y%m%d%H"), "layers": {}}

    status["layers"]["wildfires"] = current_fire_layers(
        debug,
        working_directory,
        "get_current_fire_layers.py",
        shapefile_output_directory,
    )

    status["layers"]["fire_danger"] = smokey_bear_layer(
        home_directory, working_directory, "update_smokey_bear.sh"
    )

    status["layers"]["snow_cover"] = snow_cover_layer(
        home_directory, working_directory, "update_snow_cover.sh"
    )

    status["layers"]["aqi_forecast"] = generate_daily_aqi_forecast(
        working_directory,
        "A_B_combined.py",
        aqi_forecast_netcdf_path,
        shapefile_output_directory,
    )

    status["layers"]["purpleair"] = purple_air(
        working_directory,
        "get_purple_air.py",
        purple_air_shapefile_output_directory,
    )


if __name__ == "__main__":
    update_wildfire_layers.serve(
        name="Update Wildfire Layers",
        tags=["wildfire_map"],
        parameters={
            "debug": "False",
            "home_directory": "/home/snapdata",
            "working_directory": "/usr/local/prefect/wildfire_map",
            "aqi_forecast_netcdf_path": "/usr/local/prefect/wildfire_map/aqi_forecast/netcdf_output/",
            "shapefile_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers",
            "purple_air_shapefile_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/purple_air/",
        },
    )
