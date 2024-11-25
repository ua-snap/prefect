import varnish_functions
from prefect import flow


@flow(log_prints=True)
def recache_api(url):
    varnish_functions.clone_github_repository("main", "/home/snapdata/data-api")
    varnish_functions.install_conda_environment(
        "data-api", "/home/snapdata/data-api/requirements.txt"
    )
    varnish_functions.start_flask_server(
        "data-api", "/home/snapdata/data-api/application.py"
    )
    varnish_functions.request_url(url)
    print("Finished recaching the API")


if __name__ == "__main__":
    url = "http://localhost:5000/recache/True"

    recache_api.serve(
        name="Re-cache the production API",
        tags=["API Recache"],
        parameters={
            "url": url,
        },
    )
