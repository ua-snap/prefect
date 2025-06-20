import requests
from prefect import task
from luts import eds_cached_url, ncr_cached_urls


# Helper: get the list of places to construct URLs for various endpoints,
# throw an error if there was a problem
def get_places(places_url):
    response = requests.get(places_url)
    if response.status_code == 200:
        return response.json()
    else:
        # Everything is hopeless, run away
        exit(1)


# Helper, return true if place type (community, area) matches route type (community, area).
def route_type_matches_place_type(route_type, place):
    # If the route needs a community, skip if the place isn't a community
    if route_type == "community" and place["type"] != "community":
        return False

    # If the route needs an area, skip if the place is a community
    if route_type == "area" and place["type"] == "community":
        return False

    return True


@task(name="Recache API")
def recache_api(cached_apps, cache_url):
    """Recaches the API endpoints for the given applications.

    Args:
        cached_apps - List of applications to recache, e.g., ["eds", "ncr"]
        cache_url - The base URL for the API cache, e.g., https://earthmaps.io

    Returns:
        Nothing. Prints the URLs being recached and their status code.

    """
    if "eds" in cached_apps:
        recache_eds(cache_url)

    if "ncr" in cached_apps:
        recache_ncr(cache_url)


def recache_eds(cache_url):
    # Fetch the list of places to cache: all Alaska communities
    places = get_places(cache_url + "/places/communities?tags=eds")
    get_matching_endpoints(places, eds_cached_url, "community", cache_url)


def recache_ncr(cache_url):
    places_url = cache_url + "/places/all?tags=ncr"
    places = get_places(places_url)  # has both communities (points) and areas

    # Iterate over the list of URLs that need to be cached
    for route, route_type in ncr_cached_urls.items():
        get_matching_endpoints(places, route, route_type, cache_url)


# For a list of places, request every endpoint
# where the route type (community or area) matches the place type.
# This is because some endpoints can only take areas or points.
def get_matching_endpoints(places, route, route_type, cache_url):
    for place in places:
        if route_type_matches_place_type(route_type, place):
            get_endpoint(route, place, route_type, cache_url)


def get_endpoint(route, place, route_type, cache_url):
    """Requests a specific endpoint of the API with parameters coming from
    the JSON of communities or areas.

     Args:
         route - Current route ex. /taspr/area/
         place - One community or area from the API response.
         route_type - Either community or area.
         cache_url - The base URL for the API cache, e.g., https://earthmaps.io

     Returns:
         Nothing.

    """
    # Build the URL to query based on type
    if route_type == "community":
        url = cache_url + route + str(place["latitude"]) + "/" + str(place["longitude"])
    else:
        url = cache_url + route + str(place["id"])

    print(f"Running URL: {url}")

    # Collects returned status from GET request
    status = requests.get(url)
    print(f"Status Response: {status}")
