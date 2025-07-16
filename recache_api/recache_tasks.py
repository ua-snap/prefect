import requests
import time
import os
import subprocess
import psycopg2
import re
import csv
from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from luts import eds_cached_url, ncr_cached_urls, ardac_cached_urls

status_counts = {}
error_urls = {}


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


def get_community_coords():
    if not os.path.exists("./geospatial-vector-veracity"):
        subprocess.run(
            [
                "git",
                "clone",
                "https://github.com/ua-snap/geospatial-vector-veracity.git",
            ],
            check=True,
        )
    gvv_vector_path = "./geospatial-vector-veracity/vector_data/point"

    community_coords = {}
    for filename in os.listdir(gvv_vector_path):
        if filename.endswith(".csv"):
            with open(os.path.join(gvv_vector_path, filename), "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if "id" in row and "latitude" in row and "longitude" in row:
                        community_id = row["id"]
                        lat = float(row["latitude"])
                        lon = float(row["longitude"])
                        community_coords[community_id] = {
                            "latitude": lat,
                            "longitude": lon,
                        }
    return community_coords


def connect_to_umami_db():
    umami_website_ids = (
        "1f4a98e7-d5cb-4295-82fc-5a4d41328038",  # EDS
        "2e69a077-ba5f-49c5-b076-09a44ab6fafd",  # NCR
    )
    psql_host = "umami.snap.uaf.edu"
    psql_port = "5432"
    psql_database = "umami"
    psql_user = "umami"

    # Load password from Prefect Secret
    psql_password = Secret.load("umami-psql-password").get()
    if not psql_password:
        raise ValueError("Environment variable 'PSQL_PASSWORD' is not set.")

    # Query Umami DB for community IDs
    conn = psycopg2.connect(
        database=psql_database,
        user=psql_user,
        password=psql_password,
        host=psql_host,
        port=psql_port,
    )
    cursor = conn.cursor()
    sql = """
        SELECT url_path FROM website_event WHERE website_id IN %(website_id)s
    """
    params = {"website_id": umami_website_ids}
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def sort_out_communities(rows):
    communities = []
    for row in rows:
        path = row[0]
        match = re.search(r"[A-Z]{2}\d+", path)
        if match:
            communities.append(match.group())
    communities = list(set(communities))
    communities.sort(key=lambda x: (x[:2], int(x[2:])))
    return communities


def get_frequently_used_communities():
    rows = connect_to_umami_db()
    return sort_out_communities(rows)


@task(name="Recache API")
def recache_api(cached_apps, cache_url):
    """Recaches the API endpoints for the given applications.

    Args:
        cached_apps - List of applications to recache, e.g., ["eds", "ncr", "ardac"]
        cache_url - The base URL for the API cache, e.g., https://earthmaps.io

    Returns:
        Nothing. Prints the URLs being recached and their status code.

    """
    if "eds" in cached_apps:
        recache_eds(cache_url)

    if "ncr" in cached_apps:
        recache_ncr(cache_url)

    if "ardac" in cached_apps:
        recache_ardac(cache_url)

    logger = get_run_logger()
    logger.info(f"Status code summary: {status_counts}")
    for code, urls in error_urls.items():
        logger.info(f"URLs with status {code}:")
        for url in urls:
            if code == 404 or code == 422:
                logger.warning(f"  {url}")
            else:
                logger.error(f"  {url}")


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


def recache_ardac(cache_url):
    # Uses the GVV to get community coordinates
    community_coords = get_community_coords()

    # We query the Umami database to get frequently used communities
    # This was because of the large number of communities that ARDAC can access.
    places = get_frequently_used_communities()

    for place in places:
        community_coord = community_coords.get(place)
        if not community_coord:
            continue
        for route in ardac_cached_urls:
            get_endpoint(route, community_coord, "community", cache_url)


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
    logger = get_run_logger()

    # Build the URL to query based on type
    if route_type == "community":
        url = cache_url + route.format(latitude=place["latitude"], longitude=place["longitude"])
    else:
        url = cache_url + route.format(areaID=str(place["id"]))

    logger.info(f"Running URL: {url}")

    start_time = time.perf_counter()

    # Collects returned status from GET request
    status = requests.get(url)

    end_time = time.perf_counter()

    # Count status code
    code = status.status_code

    # Initializes the status_counts diectionary for a given status code
    # and increments the count for that status code.
    status_counts[code] = status_counts.get(code, 0) + 1

    # Track any non-200 URLs
    if code != 200:
        # setdefault initializes the list if the key does not exist
        # Append the URL to the list for this status code
        error_urls.setdefault(code, []).append(url)

    # If the status code is 200, we print out the status code, the content size.
    if code == 200:
        # Size of the response content in bytes
        size_in_bytes = len(status.content)

        logger.info(
            f"Successfully recached {url}\nStatus Code: {status.status_code}\nSize of response: {size_in_bytes} bytes\nTime taken for request: {end_time - start_time:.1f} seconds"
        )
    elif code == 404:
        logger.warning(
            f"No data available at {url}\nStatus Code: {status.status_code}\nTime taken for request: {end_time - start_time:.1f} seconds"
        )
    elif code == 422:
        logger.warning(
            f"Outside of bounding box at {url}\nStatus Code: {status.status_code}\nTime taken for request: {end_time - start_time:.1f} seconds"
        )
    else:
        # Catch all for 5XX errors
        logger.error(
            f"Failed to recache {url}\nStatus Code: {status.status_code}\nTime taken for request: {end_time - start_time:.1f} seconds"
        )
