# CMIP6 regridding pipelines

The pipelines here are used for regridding large chunks of SNAP's production CMIP6 data. Currently, there are three flows: 
* `regrid-cmip6`: base flow for regridding our CMIP6 data. It may be called on its own, but it is more likely to be called as part of one of the following flows. 
* `regrid-cmip6-common`: flow for producing our production "common" / 100km CMIP6 dataset
* `regrid-cmip6-4km`: flow for regridding CMIP6 data to a 4km EPSG: 3338 grid. Likely only used as part of a downscaling flow.

The main `regrid-cmip6` flow regrids the raw CMIP6 outputs mirrored from the [Earth System Grid Federation](https://esgf.llnl.gov/) onto a common spatial grid, including extent, file structure (how data are named and grouped into files) and calendar (365-day, with no leap years), determined by some target grid file. For the `regrid-cmip6-common` flow, we use the same grid used by NCAR-CESM2, TaiESM1, and NorESM2-MM, as this was the most common grid among the models in SNAP's 12-model ensemble (not including E3SM-* models), and this ~100km resolution falls in the middle among the models. This grid is a "finite-volume grid with 0.9x1.25 degree lat/lon resolution" (note, the actual latitude resolution in the files is ~0.94°).

Below is a list of all possible variables that could be included in the a run of the regridding pipeline, for reference:

| CMIP6 variable ID | Full variable name |
|-|-|
| clt | cloud area fraction |
|evspsbl | evaporation including sublimation and transpiration|
|hfls | surface upward latent heat flux|
|hfss | surface upward sensible heat flux|
|hus | specific humidity|
|huss | near surface specific humidity|
|mrro | total runoff|
|mrsol | moisture in upper portion of soil column|
|mrsos | total water content of soil layer|
|pr | precipitation|
|prsn | snowfall flux|
|ps | surface air pressure|
|psl | sea level pressure|
|rlds | surface downwelling longwave flux in air|
|rls | surface net downward longwave flux|
|rsds | surface downwelling shortwave flux_in_air|
|rss | surface net downward shortwave flux|
|siconc | sea ice concentration|
|sithick | sea ice thickness|
|snd | surface snow thickness|
|snw | surface snow amount|
|ta | air temperature|
|tas | near surface air temperature|
|tasmax | maximum near surface air temperature|
|tasmin | minimum near surface air temperature|
|tos | sea surface temperature|
|ts | surface temperature|
|tsl | soil temperature|
|ua | eastward wind|
|uas | near surface eastward wind|
|va | northward wind|
|vas | near surface northwawrd wind|
|sfcWind | surface wind speed|
|sfcWindmax | maximum surface wind speed|
|sftlf | percentage of the grid cell occupied by land including lakes|
|sftof | sea area percentage|
|orog | surface altitude|
|zg | geopotential height at 500hPa|

