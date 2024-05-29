import os
import requests
from datetime import datetime, timezone
import json
import argparse
from osgeo import ogr

script_dir = os.path.dirname(__file__)
data_dir = os.path.join(script_dir, "data", "debug")

# All of the URLs used to generate the layers
active_fire_perimeters_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires_Perimeters/MapServer/8/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2CNAME%2CACRES%2CIRWINID%2CPRESCRIBED%2CLASTUPDATEDATETIME&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
active_fires_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=GENERALCAUSE%2COBJECTID%2CNAME%2CLASTUPDATEDATETIME%2CLATITUDE%2CLONGITUDE%2CPRESCRIBEDFIRE%2CDISCOVERYDATETIME%2CESTIMATEDTOTALACRES%2CSUMMARY%2CIRWINID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
inactive_fire_perimeters_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires_Perimeters/MapServer/10/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2CNAME%2CACRES%2CIRWINID%2CPRESCRIBED%2CLASTUPDATEDATETIME&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
inactive_fires_url = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=GENERALCAUSE%2COBJECTID%2CNAME%2CLASTUPDATEDATETIME%2CLATITUDE%2CLONGITUDE%2CDISCOVERYDATETIME%2CESTIMATEDTOTALACRES%2CSUMMARY%2COUTDATE%2CIRWINID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
viirs_12hr_url = "https://fire.gina.alaska.edu/arcgis/rest/services/afs/VIIRS_iBand_FireHeatPoints/MapServer/1/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnExceededLimitFeatures=false&quantizationParameters=&returnCentroid=false&sqlFormat=none&resultType=&featureEncoding=esriDefault&datumTransformation=&f=geojson"
viirs_24hr_url = "https://fire.gina.alaska.edu/arcgis/rest/services/afs/VIIRS_iBand_FireHeatPoints/MapServer/2/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnExceededLimitFeatures=false&quantizationParameters=&returnCentroid=false&sqlFormat=none&resultType=&featureEncoding=esriDefault&datumTransformation=&f=geojson"
viirs_48hr_url = "https://fire.gina.alaska.edu/arcgis/rest/services/afs/VIIRS_iBand_FireHeatPoints/MapServer/3/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnExceededLimitFeatures=false&quantizationParameters=&returnCentroid=false&sqlFormat=none&resultType=&featureEncoding=esriDefault&datumTransformation=&f=geojson"
todays_lightning = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Lightning/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=geojson"
yesterdays_lightning = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Lightning/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=geojson"

fire_layers_update_failed = False
lightning_layer_update_failed = False
viirs_layer_update_failed = False

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


def fetch_recent_lightning_geojson():
    if os.getenv("DEBUG") == "True":
        print("Fetching recent lightning data from local files...")
        lightning_strikes = load_local_data(f"{data_dir}/lightning.geojson")
        lightning_strikes = lightning_strikes["features"]
    else:
        print("Fetching recent lightning data from the web...")
        try:
            todays_lightning_data = fetch_data(todays_lightning)
            yesterdays_lightning_data = fetch_data(yesterdays_lightning)
            lightning_strikes = (
                todays_lightning_data["features"]
                + yesterdays_lightning_data["features"]
            )
        except:
            print(
                "Failed to fetch recent lightning data from the web. Leaving previous shapefiles intact."
            )
            global lightning_layer_update_failed
            lightning_layer_update_failed = True
            return

    return process_lightning_geojson(lightning_strikes)


def process_lightning_geojson(lightning_strikes):
    def parse_updated_time(t):
        seconds_ago = datetime.now(timezone.utc).timestamp() - (t / 1000)
        hours_difference = seconds_ago / 3600
        return hours_difference

    lightning_features = list()

    # Start by adding a few fields to each batch
    for feature in lightning_strikes:
        feature["properties"]["hoursago"] = parse_updated_time(
            feature["properties"]["UTCDATETIME"]
        )
        feature["properties"]["amplitude"] = feature["properties"]["AMPLITUDE"]
        lightning_features.append(feature)

    return lightning_features


def fetch_viirs_hotspots_geojson():
    if os.getenv("DEBUG") == "True":
        print("Fetching VIIRS hotspot data from local files...")
        viirs_data = load_local_data(f"{data_dir}/viirs.geojson")
        viirs_data = viirs_data["features"]
    else:
        print("Fetching VIIRS hotspot data from the web...")
        try:
            viirs_12hr = fetch_data(viirs_12hr_url)
            viirs_24hr = fetch_data(viirs_24hr_url)
            viirs_48hr = fetch_data(viirs_48hr_url)
            viirs_data = (
                viirs_12hr["features"] + viirs_24hr["features"] + viirs_48hr["features"]
            )
        except:
            print(
                "Failed to fetch recent lightning data from the web. Leaving previous shapefiles intact."
            )
            global viirs_layer_update_failed
            viirs_layer_update_failed = True
            return
    return viirs_data


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
            global fire_layers_update_failed
            fire_layers_update_failed = True
            return

    return process_fire_geojson(
        active_fire_perimeters, active_fires, inactive_fire_perimeters, inactive_fires
    )


def process_fire_geojson(
    activeFirePerimeters, activeFires, inactiveFirePerimeters, inactiveFires
):
    stripped_features = list()

    # Function that formats the size of the fire into the desired format
    def parse_acres(a):
        return round(float(a), 2)

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
            feature["properties"]["updated"] = feature["properties"][
                "LASTUPDATEDATETIME"
            ]

            feature["properties"]["discovered"] = feature["properties"][
                "DISCOVERYDATETIME"
            ]

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
            "updated": feature["properties"].get("updated", None),
            "OUTDATE": feature["properties"].get("OUTDATE", None),
            "discovered": feature["properties"].get("discovered", None),
        }

        # This filters out null, NaN and "0" fire sizes
        if feature["properties"]["acres"] > 0:
            stripped_features.append(feature)

    return stripped_features


def convert_geojson_to_shapefile(geojson_features, out_shapefile, feature_type="fire"):
    # Create a new Shapefile
    driver = ogr.GetDriverByName("ESRI Shapefile")
    datasource = driver.CreateDataSource(out_shapefile)

    if feature_type == "fire":
        # Create separate layers for points and polygons
        point_layer = datasource.CreateLayer("fire_points", geom_type=ogr.wkbPoint)
        polygon_layer = datasource.CreateLayer(
            "fire_polygons", geom_type=ogr.wkbPolygon
        )

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
    elif feature_type == "lightning":
        # Create a single layer for lightning points
        point_layer = datasource.CreateLayer("lightning_points", geom_type=ogr.wkbPoint)

        # Define fields for the layer
        point_layer.CreateField(ogr.FieldDefn("amplitude", ogr.OFTReal))
        point_layer.CreateField(ogr.FieldDefn("hoursago", ogr.OFTReal))
    elif feature_type == "viirs":
        # Create a single layer for VIIRS points
        point_layer = datasource.CreateLayer("viirs_hotspots", geom_type=ogr.wkbPoint)

    # Create features and add them to the respective layers
    for feature in geojson_features:
        geom_type = feature["geometry"]["type"]
        geom = ogr.CreateGeometryFromJson(json.dumps(feature["geometry"]))

        if geom_type == "Point":
            layer = point_layer
        elif geom_type in ["Polygon", "MultiPolygon"]:
            layer = polygon_layer
        else:
            continue  # Skip unsupported geometries

        feat = ogr.Feature(layer.GetLayerDefn())
        feat.SetGeometry(geom)
        try:
            if feature_type == "fire":
                feat.SetField("NAME", feature["properties"]["NAME"])
                feat.SetField("acres", feature["properties"]["acres"])
                feat.SetField("active", feature["properties"]["active"])
                feat.SetField("OUTDATE", feature["properties"]["OUTDATE"])
                feat.SetField("updated", feature["properties"]["updated"])
                feat.SetField("discovered", feature["properties"]["discovered"])
                feat.SetField("CAUSE", feature["properties"]["CAUSE"])

            elif feature_type == "lightning":
                feat.SetField("amplitude", feature["properties"]["amplitude"])
                feat.SetField("hoursago", feature["properties"]["hoursago"])

            layer.CreateFeature(feat)
        except Exception as e:
            print(f"Error adding feature to the layer: {e}")

    # Cleanup
    datasource = None


if __name__ == "__main__":
    # TODO Need a way to pass in the working directory to work with Prefect
    parser = argparse.ArgumentParser(
        description="Fetch Alaska Wildfire map fire, heat spot, and lightning data + process it."
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=f"{script_dir}/data",
        help="Directory to output shapefiles to.",
    )
    args = parser.parse_args()

    # Creates fire shapefiles
    out_shapefile = os.path.join(args.out_dir, "fires.shp")
    fire_geojson = fetch_fire_geojson()
    if fire_layers_update_failed is False:
        convert_geojson_to_shapefile(fire_geojson, out_shapefile, "fire")

    # Creates lightning shapefile
    out_shapefile = os.path.join(args.out_dir, "lightning.shp")
    lightning_geojson = fetch_recent_lightning_geojson()
    if lightning_layer_update_failed is False:
        convert_geojson_to_shapefile(lightning_geojson, out_shapefile, "lightning")

    # Creates VIIRS hotspot shapefile
    out_shapefile = os.path.join(args.out_dir, "viirs_hotspots.shp")
    viirs_hotspot_geojson = fetch_viirs_hotspots_geojson()
    if viirs_layer_update_failed is False:
        convert_geojson_to_shapefile(viirs_hotspot_geojson, out_shapefile, "viirs")

    if (
        fire_layers_update_failed
        and lightning_layer_update_failed
        and viirs_layer_update_failed
    ):
        print(
            "Failed to update any of the layers. Check for issues with pipeline and data sources."
        )

        # Exit the script with a non-zero exit code so that the Prefect flow fails
        exit(1)
