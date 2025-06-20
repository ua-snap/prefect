import requests
from prefect import task
from luts import eds_cached_url, ncr_cached_urls


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
        get_all_route_endpoints(eds_cached_url, "community", cache_url)

    if "ncr" in cached_apps:
        for route in ncr_cached_urls:
            if route.find("point") != -1 or route.find("local") != -1:
                get_all_route_endpoints(route, "community", cache_url)
            elif route.find("area") != -1:
                get_all_route_endpoints(route, "area", cache_url)


def get_all_route_endpoints(curr_route, curr_type, cache_url):
    """Generates all possible endpoints given a particular route & type

    Args:
        curr_route - Current route ex. /taspr/area/
        curr_type - Either community or area
        cache_url - The base URL for the API cache, e.g., https://earthmaps.io

    Returns:
        Nothing.
    """

    # If this is the /eds/all endpoint, we only need to cache communities
    if curr_route.find("eds") != -1:
        places_url = cache_url + "/places/communities?tags=eds"
        response = requests.get(places_url)
        places = response.json()
    else:
        if curr_type == "community":
            places_url = cache_url + "/places/communities?tags=ncr"
        else:
            places_url = cache_url + "/places/all?tags=ncr"

        response = requests.get(places_url)
        places = response.json()

    # We are excluding HUC12 areas because they add a lot of additional caching
    # that is not used in any of our apps.
    for place in places:
        # Since we are pulling from places/all, we need to skip the communities returned
        # for the area type.
        if curr_type == "area" and place["type"] == "community":
            continue

        get_endpoint(curr_route, curr_type, place, cache_url)


def get_endpoint(curr_route, curr_type, place, cache_url):
    """Requests a specific endpoint of the API with parameters coming from
    the JSON of communities or areas.

     Args:
         curr_route - Current route ex. /taspr/area/
         curr_type - Either community or area.
         place - One community or area from the API response.
         cache_url - The base URL for the API cache, e.g., https://earthmaps.io

     Returns:
         Nothing.

    """
    # Build the URL to query based on type
    if curr_type == "community":
        url = (
            cache_url
            + curr_route
            + str(place["latitude"])
            + "/"
            + str(place["longitude"])
        )
    else:
        url = cache_url + curr_route + str(place["id"])

    print(f"Running URL: {url}")

    # Collects returned status from GET request
    status = requests.get(url)
    print(f"Status Response: {status}")
