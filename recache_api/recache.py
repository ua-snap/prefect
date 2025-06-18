from prefect import flow
import recache_tasks
from luts import cached_urls


@flow(log_prints=True)
def recache_api(cached_url_list=cached_urls):
    """
    Recache API endpoints and generate performance analysis artifacts.
    
    This flow processes all configured API routes, makes requests to warm the cache,
    and automatically generates a detailed analysis report as a Prefect artifact
    visible in the dashboard.
    """
    print("🔄 Starting Prefect Recaching Flow with automatic analysis...")
    
    # Execute the recaching task which now includes analysis
    results = recache_tasks.recache_api(cached_url_list)
    
    # Enhanced status reporting
    completion_status = "🎉 COMPLETED" if results['completed_routes'] == results['total_routes'] else "⚠️ PARTIALLY COMPLETED"
    
    print(f"{completion_status} Flow finished!")
    print(f"📊 Analysis Summary:")
    print(f"   - Routes Processed: {results['completed_routes']}/{results['total_routes']}")
    print(f"   - Total Requests: {results['total_requests']:,}")
    print(f"   - Unique Routes: {results['unique_routes']}")
    print(f"   - Artifact ID: {results['artifact_id']}")
    print(f"   - View analysis in the Artifacts tab of this flow run!")
    
    return results


if __name__ == "__main__":
    recache_api.serve(
        name="Recache the data API",
        tags=["Recache API", "Analytics"],
        description="Recaches API endpoints and generates performance analysis artifacts",
        parameters={
            "cached_url_list": cached_urls,
        },
    )
