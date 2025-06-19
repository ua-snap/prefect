#! /bin/bash
# Downloads data from upstream source,
# Updates layer in GeoServer,
# Rebuilds TileCache layer.

set -x

# Default location for the output directory
output_dir="/usr/share/geoserver/data_dir/data/alaska_wildfires"

while getopts "o:" opt; do
  case $opt in
    o)
      output_dir="$OPTARG"
      ;;
    \?)
      echo "Usage: $0 [-o output_dir]"
      exit 1
      ;;
  esac
done

# Create a file in your $HOME directory containing an export
# of the admin password.
source ~/.adminpass

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

akfile="${ymd}_spruce"
akdownload="https://akff.mesowest.org/static/grids/tiff/${akfile}.tiff"
wget -nc -P /tmp ${akdownload}
akcoast="$script_dir/shapefiles/Alaska_Coast_Simplified_POLYGON.shp"
gdalwarp -crop_to_cutline -cutline ${akcoast} -t_srs EPSG:3338 /tmp/${akfile}.tiff /tmp/spruceadj_3338.tif
mv /tmp/spruceadj_3338.tif ${output_dir}

# Reseeds the tile cache
curl -v -u admin:${admin_pass} -XPOST -H "Content-type: text/xml" -d '<seedRequest><name>alaska_wildfires:spruceadj_3338</name><srs><number>3338</number></srs><zoomStart>0</zoomStart><zoomStop>7</zoomStop><format>image/png</format><type>reseed</type><threadCount>4</threadCount></seedRequest>'  "http://gs.earthmaps.io:8080/geoserver/gwc/rest/seed/alaska_wildfires:spruceadj_3338.xml"