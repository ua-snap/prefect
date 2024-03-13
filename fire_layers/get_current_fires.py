import os
import requests
import json
import argparse
from osgeo import ogr

active_fire_perimeters_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires_Perimeters/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2CNAME%2CACRES%2CIRWINID%2CPRESCRIBED&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
active_fires_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=GENERALCAUSE%2COBJECTID%2CNAME%2CLASTUPDATEDATETIME%2CLATITUDE%2CLONGITUDE%2CPRESCRIBEDFIRE%2CDISCOVERYDATETIME%2CESTIMATEDTOTALACRES%2CSUMMARY%2CIRWINID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
inactive_fire_perimeters_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires_Perimeters/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2CNAME%2CACRES%2CIRWINID%2CPRESCRIBED&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
inactive_fires_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=GENERALCAUSE%2COBJECTID%2CNAME%2CLASTUPDATEDATETIME%2CLATITUDE%2CLONGITUDE%2CDISCOVERYDATETIME%2CESTIMATEDTOTALACRES%2CSUMMARY%2COUTDATE%2CIRWINID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"

data_dir = "./data/debug"  # Directory to look for local data files

if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def fetch_data(url):
    response = requests.get(url)

    # If the response from the URL is not 200, raise an error
    response.raise_for_status()
    return response.json()


def load_local_data(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def save_local_data(data, file_path):
    with open(file_path, "w") as f:
        json.dump(data, f)


def get_fire_geojson():
    try:
        return fetch_fire_geojson()
    except:
        print("Failed to get the data")


def fetch_fire_geojson():
    if os.getenv("DEBUG") == "True":
        print("Fetching fire data from local files...")
        active_fire_perimeters = load_local_data(
            f"{data_dir}/activeFirePerimeters.geojson"
        )
        active_fires = load_local_data(f"{data_dir}/activeFires.geojson")
        inactive_fire_perimeters = load_local_data(
            f"{data_dir}/inactiveFirePerimeters.geojson"
        )
        inactive_fires = load_local_data(f"{data_dir}/inactiveFires.geojson")
    else:
        print("Fetching fire data from the web...")
        try:
            active_fire_perimeters = fetch_data(active_fire_perimeters_url)
            active_fires = fetch_data(active_fires_url)
            inactive_fire_perimeters = fetch_data(inactive_fire_perimeters_url)
            inactive_fires = fetch_data(inactive_fires_url)
        except:
            print(
                "Failed to fetch fire data from the web. Leaving previous shapefiles intact."
            )
            exit(0)

    return process_fire_geojson(
        active_fire_perimeters, active_fires, inactive_fire_perimeters, inactive_fires
    )


def process_fire_geojson(
    activeFirePerimeters, activeFires, inactiveFirePerimeters, inactiveFires
):
    stripped_features = []

    # Function that formats the size of the fire into the desired format
    def parse_acres(a):
        return round(float(a), 2)

    # Function that formats the update time into the desired format
    def parse_updated_time(t):
        # No-op for now, you can implement this if needed
        return t

    # Start by adding a few fields to each batch
    for features in [
        activeFirePerimeters["features"],
        inactiveFirePerimeters["features"],
    ]:
        for feature in features:
            feature["properties"]["active"] = (
                True if features is activeFirePerimeters["features"] else False
            )
            feature["properties"]["acres"] = parse_acres(feature["properties"]["ACRES"])
            # GENERALCAUSE is not always present in the data and it is also too long a field name for a shapefile
            if "GENERALCAUSE" not in feature["properties"]:
                feature["properties"]["CAUSE"] = "N/A"

    for features in [activeFires["features"], inactiveFires["features"]]:
        for feature in features:
            feature["properties"]["active"] = (
                True if features is activeFires["features"] else False
            )
            if features is activeFires["features"]:
                feature["properties"]["OUTDATE"] = None
            feature["properties"]["acres"] = parse_acres(
                feature["properties"]["ESTIMATEDTOTALACRES"]
            )
            feature["properties"]["updated"] = parse_updated_time(
                feature["properties"]["LASTUPDATEDATETIME"]
            )
            feature["properties"]["discovered"] = parse_updated_time(
                feature["properties"]["DISCOVERYDATETIME"]
            )
            # GENERALCAUSE is not always present in the data and it is also too long a field name for a shapefile
            if (
                "GENERALCAUSE" not in feature["properties"]
                or feature["properties"]["GENERALCAUSE"] is None
            ):
                feature["properties"]["CAUSE"] = "N/A"
            else:
                feature["properties"]["CAUSE"] = feature["properties"]["GENERALCAUSE"]

    # Create a temporary data structure that is indexed in a useful way
    indexed_fire_perimeters = {
        feature["properties"]["IRWINID"]: feature
        for feature in activeFirePerimeters["features"]
        + inactiveFirePerimeters["features"]
    }
    indexed_all_fires = {
        feature["properties"]["IRWINID"]: feature
        for feature in activeFires["features"] + inactiveFires["features"]
    }

    # Combine fire info with a perimeter, if available
    merged_features = []
    for key, feature in indexed_all_fires.items():
        temp_feature = feature.copy()
        if key in indexed_fire_perimeters:
            # Grab polygon and add to other fire data
            temp_feature["geometry"] = indexed_fire_perimeters[key]["geometry"]
        merged_features.append(temp_feature)

    # Sometimes, there's a fire perimeter but no way to tie it to the other list of info
    for key, feature in indexed_fire_perimeters.items():
        if key not in indexed_all_fires:
            merged_features.append(feature)

    # Finally, flush any fields that we're not using in the GUI
    for feature in merged_features:
        feature["properties"] = {
            "active": feature["properties"]["active"],
            "NAME": feature["properties"]["NAME"],
            "acres": feature["properties"]["acres"],
            "CAUSE": feature["properties"]["CAUSE"],
            "updated": feature["properties"]["updated"],
            "OUTDATE": feature["properties"]["OUTDATE"],
            "discovered": feature["properties"]["discovered"],
        }

        # This filters out null, NaN and "0" fire sizes
        if feature["properties"]["acres"] > 0:
            stripped_features.append(feature)

    return stripped_features


def convert_geojson_to_shapefile(geojson_features, out_shapefile):
    # Create a new Shapefile
    driver = ogr.GetDriverByName("ESRI Shapefile")
    datasource = driver.CreateDataSource(out_shapefile)

    # Create separate layers for points and polygons
    point_layer = datasource.CreateLayer("fire_points", geom_type=ogr.wkbPoint)
    polygon_layer = datasource.CreateLayer("fire_polygons", geom_type=ogr.wkbPolygon)

    # Define fields for both layers
    point_layer.CreateField(ogr.FieldDefn("NAME", ogr.OFTString))
    point_layer.CreateField(ogr.FieldDefn("acres", ogr.OFTReal))
    point_layer.CreateField(ogr.FieldDefn("active", ogr.OFTString))
    point_layer.CreateField(ogr.FieldDefn("OUTDATE", ogr.OFTString))
    point_layer.CreateField(ogr.FieldDefn("updated", ogr.OFTString))
    point_layer.CreateField(ogr.FieldDefn("CAUSE", ogr.OFTString))
    point_layer.CreateField(ogr.FieldDefn("discovered", ogr.OFTString))

    polygon_layer.CreateField(ogr.FieldDefn("NAME", ogr.OFTString))
    polygon_layer.CreateField(ogr.FieldDefn("acres", ogr.OFTReal))
    polygon_layer.CreateField(ogr.FieldDefn("active", ogr.OFTString))
    polygon_layer.CreateField(ogr.FieldDefn("OUTDATE", ogr.OFTString))
    polygon_layer.CreateField(ogr.FieldDefn("updated", ogr.OFTString))
    polygon_layer.CreateField(ogr.FieldDefn("CAUSE", ogr.OFTString))
    polygon_layer.CreateField(ogr.FieldDefn("discovered", ogr.OFTString))

    # Create features and add them to the respective layers
    for feature in geojson_features:
        geom_type = feature["geometry"]["type"]
        geom = ogr.CreateGeometryFromJson(json.dumps(feature["geometry"]))

        if geom_type == "Point":
            layer = point_layer
        elif geom_type == "Polygon":
            layer = polygon_layer
        else:
            continue  # Skip unsupported geometries

        feat = ogr.Feature(layer.GetLayerDefn())
        feat.SetGeometry(geom)
        try:
            feat.SetField("NAME", feature["properties"]["NAME"])
            feat.SetField("acres", feature["properties"]["acres"])
            feat.SetField("active", feature["properties"]["active"])
            feat.SetField("OUTDATE", feature["properties"]["OUTDATE"])
            feat.SetField("updated", feature["properties"]["updated"])
            feat.SetField("discovered", feature["properties"]["discovered"])
            feat.SetField("CAUSE", feature["properties"]["CAUSE"])
            layer.CreateFeature(feat)
        except Exception as e:
            print(f"Error adding feature to the layer: {e}")

    # Cleanup
    datasource = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch fire data and process it.")
    parser.add_argument(
        "--out-dir",
        type=str,
        default="./data",
        help="Directory to output GeoTIFF files to.",
    )
    args = parser.parse_args()

    out_shapefile = os.path.join(args.out_dir, "fires.shp")

    fire_geojson = fetch_fire_geojson()
    convert_geojson_to_shapefile(fire_geojson, out_shapefile)
