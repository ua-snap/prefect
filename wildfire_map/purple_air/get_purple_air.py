import requests
import argparse
import geopandas as gpd
from shapely.geometry import Point
import os
from datetime import datetime, timedelta

script_dir = os.path.dirname(__file__)


def calculate_aqi(pm25_concentration):
    """
    Calculate the AQI value for a given PM2.5 concentration.
    The AQI is calculated based on the following breakpoints for PM2.5 concentration:
    - Good: 0 - 12.0
    - Moderate: 12.1 - 35.4
    - Unhealthy for Sensitive Groups: 35.5 - 55.4
    - Unhealthy: 55.5 - 150.4
    - Very Unhealthy: 150.5 - 250.4
    - Hazardous: 250.5 - 350.4
    - Very Hazardous: 350.5 - 500.4
    - Max AQI: 500

    Taken from this document: https://www.airnow.gov/sites/default/files/2020-05/aqi-technical-assistance-document-sept2018.pdf
    """
    if pm25_concentration <= 12.0:
        return ((50 - 0) / (12.0 - 0)) * (pm25_concentration - 0) + 0
    elif pm25_concentration <= 35.4:
        return ((100 - 51) / (35.4 - 12.1)) * (pm25_concentration - 12.1) + 51
    elif pm25_concentration <= 55.4:
        return ((150 - 101) / (55.4 - 35.5)) * (pm25_concentration - 35.5) + 101
    elif pm25_concentration <= 150.4:
        return ((200 - 151) / (150.4 - 55.5)) * (pm25_concentration - 55.5) + 151
    elif pm25_concentration <= 250.4:
        return ((300 - 201) / (250.4 - 150.5)) * (pm25_concentration - 150.5) + 201
    elif pm25_concentration <= 350.4:
        return ((400 - 301) / (350.4 - 250.5)) * (pm25_concentration - 250.5) + 301
    elif pm25_concentration <= 500.4:
        return ((500 - 401) / (500.4 - 350.5)) * (pm25_concentration - 350.5) + 401
    else:
        return 500  # Max AQI


def fetch_purple_air_data():
    one_week_ago = int((datetime.now() - timedelta(days=7)).timestamp())

    api_url = "https://map.purpleair.com/v1/sensors"
    query_params = {
        "fields": "last_seen,location_type,latitude,longitude,pm2.5_10minute,pm2.5_24hour",
        "modified_since": one_week_ago,
        "nwlat": 70,
        "selat": 54.56,
        "nwlng": -169.41,
        "selng": -130.1,
    }

    headers = {"X-API-KEY": os.getenv("PURPLE_AIR_API_KEY")}

    response = requests.get(api_url, params=query_params, headers=headers)
    response.raise_for_status()

    data = response.json()

    for sensor in data["data"]:
        sensor.append(0)  # AQI 2.5 PM 1 hour
        sensor.append("pa")  # type

    return data


def fetch_dec_air_data():
    api_url = "https://services.arcgis.com/8MMg7skvEbOESlSM/ArcGIS/rest/services/AQ_Sites_Public/FeatureServer/0/query?where=1=1&outFields=*&f=geojson"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    returned_data = list()

    # Recategorize or omit sensors based on their sensor_id.
    # More info here: https://github.com/ua-snap/alaska-wildfires/issues/224
    conocophillips_sensors = [
        "Nuiqsut",
    ]
    blm_sensors = [
        "BLM_Kaktovik",
    ]
    louden_tribe_sensors = [
        "Quant_MOD00758",
        "Quant_MOD00759",
    ]
    sensors_to_omit = [
        "Quant_MOD00443",
        "Quant_MOD00463",
        "Quant_MOD00471",
        "Quant_MOD00651",
        "Quant_MOD00652",
        "Quant_MOD00665",
    ]

    for feature in data["features"]:
        # If pm25calibrated is None, skip this DEC sensor
        if feature["properties"]["pm25calibrated"] is None:
            continue

        sensor_id = feature["properties"]["sensor_id"]
        if sensor_id in sensors_to_omit:
            continue

        new_feature = list()
        new_feature.append(feature["properties"]["OBJECTID"])  # objectId
        new_feature.append(
            round(feature["properties"]["time_stamp"] / 1000)
        )  # lastupdate
        new_feature.append(0)  # outdoor sensor
        new_feature.append(feature["geometry"]["coordinates"][1])  # latitude
        new_feature.append(feature["geometry"]["coordinates"][0])  # longitude
        new_feature.append(0)  # pm2.5_10m
        new_feature.append(0)  # pm2.5_24hr
        new_feature.append(
            feature["properties"]["pm25calibrated"]
        )  # AQI PM2.5 1hr, this field is already converted to AQI

        # Assign type for each sensor
        if sensor_id in conocophillips_sensors:
            new_feature.append("conocophillips")
        elif sensor_id in blm_sensors:
            new_feature.append("blm")
        elif sensor_id in louden_tribe_sensors:
            new_feature.append("louden_tribe")
        else:
            new_feature.append("dec")

        returned_data.append(new_feature)

    return returned_data


def create_shapefile(data, output_path):
    records = []
    for sensor in data["data"]:
        # If sensor type is indicated as inside, skip it
        # 0 = outside, 1 = inside
        if sensor[2] == 1:
            continue
        lon = sensor[4]
        lat = sensor[3]

        properties = {
            "lastupdate": sensor[1],
            "pm2_5_10m": round(sensor[5]),
            "aqi_10m": round(calculate_aqi(sensor[5])),
            "pm2_5_24hr": round(sensor[6]),
            "aqi_24hr": round(calculate_aqi(sensor[6])),
            "aqi_1hr": round(sensor[7]),
            "type": sensor[8],
        }

        point = Point(lon, lat)
        records.append({"geometry": point, **properties})

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")

    gdf = gdf.to_crs(epsg=3338)

    gdf.to_file(output_path, driver="ESRI Shapefile")


def main(out_dir):
    data = fetch_purple_air_data()

    # Add DEC Air data to the Purple Air data
    data["data"] = data["data"] + fetch_dec_air_data()

    output_path = os.path.join(out_dir, "purple_air_pm25.shp")
    create_shapefile(data, output_path)
    print(f"Shapefile created at {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Purple Air AQI data with DEC Air data + process it."
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=f"{script_dir}/",
        help="Directory to output shapefile to.",
    )
    args = parser.parse_args()
    main(args.out_dir)
