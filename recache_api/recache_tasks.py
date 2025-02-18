import requests
from prefect import task


@task(name="Recache API")
def recache_api(cached_urls):

    for route in cached_urls:
        if (
            route.find("point") != -1
            or route.find("local") != -1
            or route.find("all") != -1
        ) and (route.find("lat") == -1):
            get_all_route_endpoints(route, "community")
        elif route.find("area") != -1 and route.find("var_id") == -1:
            get_all_route_endpoints(route, "area")


def get_all_route_endpoints(curr_route, curr_type):
    """Generates all possible endpoints given a particular route & type

    Args:
        curr_route - Current route ex. https://earthmaps.io/taspr/huc/
        curr_type - One of the many types availabe such as community or huc.

    Returns:
        Nothing.
    """
    GS_BASE_URL = "https://gs.earthmaps.io/geoserver/"

    # Opens the JSON file for the current type and replaces the "variable" portions
    # of the route to allow for the JSON items to fill in those fields.
    if curr_type == "community":
        places_url = (
            GS_BASE_URL
            + f"wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=all_boundaries:all_communities&outputFormat=application/json&propertyName=latitude,longitude"
        )
        response = requests.get(places_url)
        places_data = response.json()
        places = places_data["features"]
    else:
        places_url = (
            GS_BASE_URL
            + f"wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=all_boundaries:all_areas&outputFormat=application/json&propertyName=id"
        )
        response = requests.get(places_url)
        places_data = response.json()
        places = places_data["features"]

    # For each JSON item in the JSON object array
    for place in places:
        get_endpoint(curr_route, curr_type, place["properties"])


def get_endpoint(curr_route, curr_type, place):
    """Requests a specific endpoint of the API with parameters coming from
    the JSON of communities, HUCs, or protected areas.

     Args:
         curr_route - Current route ex. https://earthmaps.io/taspr/huc/
         curr_type - One of three types: community, huc, or pa
         place - One item of the JSON for community, huc, or protected area

     Returns:
         Nothing.

    """
    # Build the URL to query based on type
    if curr_type == "community":
        url = (
            "https://earthmaps.io"
            + curr_route
            + str(place["latitude"])
            + "/"
            + str(place["longitude"])
        )
    else:
        url = "https://earthmaps.io" + curr_route + str(place["id"])

    print(f"Running URL: {url}")

    # Collects returned status from GET request
    status = requests.get(url)
    print(f"Status Response: {status}")
