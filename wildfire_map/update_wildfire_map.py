from prefect import flow
from prefect.blocks.system import Secret
from fire_layers.current_fire_layers import current_fire_layers
from smokey_bear.smokey_bear_layer import smokey_bear_layer
from smokey_bear.snow_cover_layer import snow_cover_layer
from aqi_forecast.generate_daily_aqi_forecast import generate_daily_aqi_forecast
from purple_air.get_daily_purple_air import purple_air
from datetime import datetime
import boto3
import json


@flow(log_prints=True)
def update_wildfire_layers(
    debug,
    home_directory,
    working_directory,
    aqi_forecast_netcdf_path,
    shapefile_output_directory,
    purple_air_shapefile_output_directory,
):
    wildfire_status_access_key = Secret.load("wildfire-status-access-key")
    wildfire_secret_access_key = Secret.load("wildfire-secret-access-key")

    aqi_forecast_hour = "00" if 8 <= datetime.now().hour < 20 else "12"

    status = {"updated": datetime.now().strftime("%Y%m%d%H"), "layers": {}}

    status["layers"]["wildfires"] = current_fire_layers(
        debug,
        working_directory,
        "get_current_fire_layers.py",
        shapefile_output_directory,
    )

    print("Finished updating wildfire layers")
    print(status["layers"]["wildfires"])

    status["layers"]["fire_danger"] = smokey_bear_layer(
        home_directory, working_directory, "update_smokey_bear.sh"
    )

    print("Finished updating fire danger layer")
    print(status["layers"]["fire_danger"])

    status["layers"]["snow_cover"] = snow_cover_layer(
        home_directory, working_directory, "update_snow_cover.sh"
    )

    print("Finished updating snow cover layer")
    print(status["layers"]["snow_cover"])

    status["layers"]["aqi_forecast"] = generate_daily_aqi_forecast(
        working_directory,
        "A_B_combined.py",
        aqi_forecast_netcdf_path,
        shapefile_output_directory,
        aqi_forecast_hour,
    )

    print("Finished updating AQI forecast")
    print(status["layers"]["aqi_forecast"])

    status["layers"]["purpleair"] = purple_air(
        working_directory,
        "get_purple_air.py",
        purple_air_shapefile_output_directory,
    )

    print("Finished updating Purple Air layer")
    print(status["layers"]["purpleair"])

    status_json = json.dumps(status)

    # Initialize a session using your wildfire credentials
    session = boto3.Session(
        aws_access_key_id=wildfire_status_access_key.get(),
        aws_secret_access_key=wildfire_secret_access_key.get(),
    )

    # Initialize the S3 client
    s3 = session.client("s3")

    # Specify the S3 bucket name and the key (path) for the uploaded file
    bucket_name = "alaskawildfires.org"
    key = "status.json"

    # Upload the JSON string as a file to the S3 bucket
    print(f"Uploading status.json to s3://{bucket_name}/{key}")
    s3.put_object(Bucket=bucket_name, Key=key, Body=status_json, ACL="public-read")

    print("Finished updating wildfire layers")


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
