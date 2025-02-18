from prefect import flow
import recache_tasks
from luts import cached_urls


@flow(log_prints=True)
def recache_api(cached_url_list=cached_urls):
    recache_tasks.run_recache(cached_url_list)


if __name__ == "__main__":
    recache_api.serve(
        name="Recache the data API",
        tags=["Recache API"],
        parameters={
            "cached_url_list": cached_urls,
        },
    )
