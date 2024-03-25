from prefect import flow
from fire_layers.current_fire_layers import current_fire_layers
from smokey_bear.smokey_bear_layer import smokey_bear_layer
from smokey_bear.snow_cover_layer import snow_cover_layer


@flow(log_prints=True)
def update_wildfire_layers(debug, working_directory, shapefile_output_directory):
    current_fire_layers(
        debug,
        working_directory,
        "get_current_fire_layers.py",
        shapefile_output_directory,
    )

    smokey_bear_layer(working_directory, "update_smokey_bear.sh")

    snow_cover_layer(working_directory, "update_snow_cover.sh")


if __name__ == "__main__":
    update_wildfire_layers.serve(
        name="Update Wildfire Layers",
        tags=["wildfire_map"],
        parameters={
            "debug": "False",
            "working_directory": "/usr/local/prefect",
            "shapefile_output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/fire_layers",
        },
    )
