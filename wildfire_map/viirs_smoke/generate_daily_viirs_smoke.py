from prefect import flow
from .viirs_smoke_tasks import *
import datetime


@flow(log_prints=True)
def generate_viirs_smoke(working_directory, script_name, output_directory):
    try:
        install_conda_environment("viirs_smoke", f"{working_directory}/environment.yml")

        execute_local_script(f"{working_directory}/{script_name}", output_directory)
        return {"updated": datetime.now().strftime("%Y%m%d%H"), "succeeded": True}
    except Exception as e:
        return {
            "updated": datetime.now().strftime("%Y%m%d%H"),
            "succeeded": False,
            "error": str(e),
        }


if __name__ == "__main__":
    generate_viirs_smoke.serve(
        name="Generate Daily VIIRS Smoke Layer for Wildfire Map",
        tags=["VIIRS Smoke", "Wildfire Map"],
        parameters={
            "working_directory": "/usr/local/prefect/wildfire_map/viirs_smoke",
            "script_name": "create_viirs_smoke.py",
            "output_directory": "/usr/share/geoserver/data_dir/data/alaska_wildfires/",
        },
    )
