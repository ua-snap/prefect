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

    return response.json()


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
        }

        point = Point(lon, lat)
        records.append({"geometry": point, **properties})

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")

    gdf = gdf.to_crs(epsg=3338)

    gdf.to_file(output_path, driver="ESRI Shapefile")


def main(out_dir):
    data = fetch_purple_air_data()
    output_path = os.path.join(out_dir, "purple_air_pm25.shp")
    create_shapefile(data, output_path)
    print(f"Shapefile created at {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Purple Air AQI data + process it."
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=f"{script_dir}/",
        help="Directory to output shapefile to.",
    )
    args = parser.parse_args()
    main(args.out_dir)
