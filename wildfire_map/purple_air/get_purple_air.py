import requests
import argparse
import geopandas as gpd
from shapely.geometry import Point
import os
from datetime import datetime, timedelta

script_dir = os.path.dirname(__file__)


def fetch_purple_air_data():
    one_week_ago = int((datetime.now() - timedelta(days=7)).timestamp())

    api_url = "https://map.purpleair.com/v1/sensors"
    query_params = {
        "fields": "last_seen,location_type,latitude,longitude,pm2.5,pm2.5_10minute,pm2.5_30minute,pm2.5_60minute,pm2.5_6hour,pm2.5_24hour,pm2.5_1week",
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
        lon = sensor[4]
        lat = sensor[3]

        properties = {
            "lastupdate": sensor[1],
            "type": sensor[2],
            "pm2_5": sensor[5],
            "pm2_5_10m": sensor[6],
            "pm2_5_30m": sensor[7],
            "pm2_5_60m": sensor[8],
            "pm2_5_6hr": sensor[9],
            "pm2_5_24hr": sensor[10],
            "pm2_5_1wk": sensor[11],
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
