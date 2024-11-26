import sys
import types
from prefect import flow
import ingest_tasks

# Import all the coverage flows
from air_freezing_index_Fdays import air_freezing_index_Fdays
from air_thawing_index_Fdays import air_thawing_index_Fdays
from alfresco_relative_flammability_30yr import alfresco_relative_flammability_30yr
from alfresco_vegetation_type_percentage import alfresco_vegetation_type_percentage
from annual_precip_totals_mm import annual_precip_totals_mm
from annual_mean_snowfall import annual_mean_snowfall
from annual_mean_temp import annual_mean_temp
from ardac_beaufort_daily_slie import ardac_beaufort_daily_slie
from ardac_beaufort_landfast_sea_ice_mmm import ardac_beaufort_landfast_sea_ice_mmm
from ardac_chukchi_daily_slie import ardac_chukchi_daily_slie
from ardac_chukchi_landfast_sea_ice_mmm import ardac_chukchi_landfast_sea_ice_mmm
from beetle_risk import beetle_risk
from cmip6_indicators import cmip6_indicators
from cmip6_monthly import cmip6_monthly
from crrel_gipl_outputs import crrel_gipl_outputs
from degree_days_below_zero_Fdays import degree_days_below_zero_Fdays
from dot_precip import dot_precip
from heating_degree_days_Fdays import heating_degree_days_Fdays
from hsia_arctic_production import hsia_arctic_production
from hydrology import hydrology
from iem_ar5_2km_taspr_seasonal import iem_ar5_2km_taspr_seasonal
from iem_cru_2km_taspr_seasonal_baseline_stats import (
    iem_cru_2km_taspr_seasonal_baseline_stats,
)
from jan_min_mean_max_temp import jan_min_mean_max_temp
from july_min_mean_max_temp import july_min_mean_max_temp
from ncar12km_indicators_era_summaries import ncar12km_indicators_era_summaries
from tas_2km_historical import tas_2km_historical
from tas_2km_projected import tas_2km_projected
from wet_days_per_year import wet_days_per_year


@flow(log_prints=True)
def reingest_all_coverages(branch_name, reingest_these_coverages, delete_coverages):
    for coverage_name in reingest_these_coverages:
        coverage_func = globals().get(coverage_name)
        if callable(coverage_func):
            if delete_coverages:
                ingest_tasks.delete_coverage(coverage_name)

            coverage_func(branch_name=branch_name)


if __name__ == "__main__":
    import_names = [
        name
        for name, obj in globals().items()
        if not name.startswith("__")
        and not name.startswith("flow")
        and not name.startswith("reingest_all_coverages")
        and not isinstance(obj, types.ModuleType)
    ]

    # Output the dynamic list of module names
    print(import_names)

    reingest_all_coverages.serve(
        name="Re-ingest all Rasdaman coverages",
        tags=["Rasdaman", "Re-ingest"],
        parameters={
            "branch_name": "main",
            "reingest_these_coverages": import_names,
            "delete_coverages": False,
        },
    )
