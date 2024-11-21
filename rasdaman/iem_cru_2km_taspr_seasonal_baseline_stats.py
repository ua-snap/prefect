from prefect import flow
import ingest_tasks


@flow(log_prints=True)
def iem_cru_2km_taspr_seasonal_baseline_stats(
    branch_name="main",
    working_directory="/opt/rasdaman/user_data/snapdata/",
    ingest_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_pr_2km/",
    source_directory="/workspace/Shared/Tech_Projects/IEM/cru_ts40_2km_monthly_taspr_iem_domain/",
    data_directory="/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_pr_2km/cru_baseline_data/",
):
    ingest_tasks.clone_github_repository(branch_name, working_directory)

    ingest_tasks.check_for_nfs_mount("/workspace/Shared")

    ingest_tasks.copy_data_from_nfs_mount(
        source_directory,
        data_directory,
        only_files=True,
    )

    ingest_tasks.run_ingest(
        ingest_directory, ingest_file="cru_seasonal_baseline_ingest.json"
    )


if __name__ == "__main__":
    iem_cru_2km_taspr_seasonal_baseline_stats.serve(
        name="Rasdaman Coverage: iem_cru_2km_taspr_seasonal_baseline_stats",
        tags=["IEM", "Temperature", "Precipitation", "CRU TS"],
        parameters={
            "branch_name": "main",
            "working_directory": "/opt/rasdaman/user_data/snapdata/",
            "ingest_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_pr_2km/",
            "source_directory": "/workspace/Shared/Tech_Projects/IEM/cru_ts40_2km_monthly_taspr_iem_domain/",
            "data_directory": "/opt/rasdaman/user_data/snapdata/rasdaman-ingest/iem/tas_pr_2km/cru_baseline_data/",
        },
    )
