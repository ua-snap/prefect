#! /bin/bash
# Downloads data from upstream source,
# Updates layer in GeoServer,
# Rebuilds TileCache layer.

set -x

# Create a file in your $HOME directory containing an export
# of the admin password.
source ~/.adminpass

# Activates the smokeybear Conda environment with GDAL installed.
source /opt/miniconda3/bin/activate
conda activate /home/snapdata/.conda/envs/smokeybear

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
	ymd=$( date -d "yesterday" '+%Y%m%d' )
	year=$( date -d "yesterday" '+%Y' )
elif [[ "$OSTYPE" == "darwin"* ]]; then
    ymd=$( date -v-1d +%Y%m%d )
    year=$( date -v-1d +%Y )
else
	echo "OS unknown?"
	exit 1
fi

script_dir=$(dirname "$(readlink -f "$0")")
GEOSERVER_HOME=/usr/share/geoserver

akfile="${ymd}_spruce"
akdownload="https://akff.mesowest.org/static/grids/tiff/${akfile}.tiff"
wget -nc -P /tmp ${akdownload}
akcoast="$script_dir/shapefiles/Alaska_Coast_Simplified_POLYGON.shp"
gdalwarp -crop_to_cutline -cutline ${akcoast} -t_srs EPSG:3338 /tmp/${akfile}.tiff /tmp/spruceadj_3338.tif
mv /tmp/spruceadj_3338.tif $GEOSERVER_HOME/data_dir/data/alaska_wildfires/

# Reseeds the tile cache
curl -v -u admin:${admin_pass} -XPOST -H "Content-type: text/xml" -d '<seedRequest><name>alaska_wildfires:spruceadj_3338</name><srs><number>3338</number></srs><zoomStart>0</zoomStart><zoomStop>7</zoomStop><format>image/png</format><type>reseed</type><threadCount>4</threadCount></seedRequest>'  "http://gs.mapventure.org:8080/geoserver/gwc/rest/seed/alaska_wildfires:spruceadj_3338.xml"