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

fname="NIC.IMS_v3_${ymd}_1km.tif"
tname="snow_cover_3338.tif"
wget -nc --content-disposition -O "/tmp/$fname.gz" "https://usicecenter.gov/File/DownloadProduct?products=%2Fims%2Fims_v3%2Fimstif%2F1km%2F${year}&fName=${fname}.gz"
gunzip -f /tmp/$fname.gz
gdal_translate -projwin -175.0 50.0 -80.0 55.0 -projwin_srs EPSG:4326 /tmp/$fname /tmp/snow_cover_crop.tif
akcoast="$script_dir/shapefiles/Alaska_Coast_Simplified_POLYGON.shp"
gdalwarp -crop_to_cutline -cutline ${akcoast} -overwrite -t_srs EPSG:3338 /tmp/snow_cover_crop.tif /tmp/snow_cover_warp.tif
gdal_translate -projwin 173.2 77.0 -118.0 46.0 -projwin_srs EPSG:4326 /tmp/snow_cover_warp.tif /tmp/$tname
mv /tmp/$tname $GEOSERVER_HOME/data_dir/data/alaska_wildfires/

# Reseeds tile cache layer
curl -v -u admin:${admin_pass} -XPOST -H "Content-type: text/xml" -d '<seedRequest><name>alaska_wildfires:snow_cover_3338</name><srs><number>3338</number></srs><zoomStart>0</zoomStart><zoomStop>7</zoomStop><format>image/png</format><type>reseed</type><threadCount>4</threadCount></seedRequest>'  "http://gs.earthmaps.io:8080/geoserver/gwc/rest/seed/alaska_wildfires:snow_cover_3338.xml"