# CMIP6 regridding pipeline

The `regrid_cmip6` flow is used for generating SNAP's production CMIP6 dataset. This is simply standardizing all raw CMIP6 outputs mirrored from the [Earth System Grid Federation](https://esgf.llnl.gov/) onto a common spatial grid, including file structure (how data are named and grouped into files) and calendar (365-day, with no leap years). We use the same grid used by NCAR-CESM2, TaiESM1, and NorESM2-MM, as this was the most common grid among the models in SNAP's 12-model ensemble. This grid is a "finite-volume grid with 0.9x1.25 degree lat/lon resolution" (note, the actual latitude resolution in the files is ~0.94°).

## Production config files

You can use the following config files to do "production runs" of the regridding pipeline. 
In the prefect flow page, do a "custom" run, and copy the contents of the file into the "JSON" box. Make sure to change the `ssh_private_key_path` to your own key. 

`v1_1_config.json` - This is the "V1" variables from our scoping doc, but arguably the most useful variables from that batch - air temperature, precip, and wind. 
`v1_2_config.json` - These are the remaining "V1" variables from our scoping doc. Includes wind components, radiation variables, etc.  

### Variable list

Below is a list of all possible variables that could be included in the a run of the regridding pipeline, for reference. They have been split out according to the config files herein. 

#### V1 variables

**part 1**

| CMIP6 variable ID | Full variable name |
|-|-|
|pr | precipitation|
|tas | near surface air temperature|
|tasmax | maximum near surface air temperature|
|tasmin | minimum near surface air temperature|
|sfcWind | surface wind speed|
|sfcWindmax | maximum surface wind speed|

**part 2**

| CMIP6 variable ID | Full variable name |
|-|-|
| clt | cloud area fraction |
|evspsbl | evaporation including sublimation and transpiration|
|hfls | surface upward latent heat flux|
|hfss | surface upward sensible heat flux|
|psl | sea level pressure|
|rlds | surface downwelling longwave flux in air|
|rls | surface net downward longwave flux|
|rsds | surface downwelling shortwave flux_in_air|
|rss | surface net downward shortwave flux|
|ts | surface temperature|
|uas | near surface eastward wind|
|vas | near surface northwawrd wind|

#### Remainder (unallocated)

| CMIP6 variable ID | Full variable name |
|-|-|
|hus | specific humidity|
|huss | near surface specific humidity|
|mrro | total runoff|
|mrsol | moisture in upper portion of soil column|
|mrsos | total water content of soil layer|
|prsn | snowfall flux|
|ps | surface air pressure|
|siconc | sea ice concentration|
|sithick | sea ice thickness|
|snd | surface snow thickness|
|snw | surface snow amount|
|ta | air temperature|
|tos | sea surface temperature|
|tsl | soil temperature|
|ua | eastward wind|
|va | northward wind|
|sftlf | percentage of the grid cell occupied by land including lakes|
|sftof | sea area percentage|
|orog | surface altitude|
|zg | geopotential height at 500hPa|
