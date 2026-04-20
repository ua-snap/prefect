from prefect import flow
import recache_tasks


@flow(log_prints=True)
def recache_api(cached_apps, cache_url):
    recache_tasks.recache_api(cached_apps, cache_url)


if __name__ == "__main__":
    recache_api.serve(
        name="Recache the data API",
        tags=["Recache API"],
        parameters={
            "cached_apps": ["eds", "ncr", "ardac"],
            "cache_url": "https://earthmaps.io",
        },
    )
