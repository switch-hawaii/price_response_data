
********************
TODO:
- find re_supply_curve.csv from this vintage and use that to make a supply curve
  graph that matches the study; add that to the repository
- see if the 2019 archive has 2007-08 irradiance data from later than 2016 that
  we may have used to make the DistPV data (maybe version 2 or 3 of NSRDB
  database?). See if we are using exactly the right version of k_means here
  (may have been a fixed one a few hours later?) Figure out why we can't
  "unmix" the distributed PV sites from the distributed PV projects.
*****************

This repository contains data and code used to create the data warehouse for Switch
models of the Oahu power system.

Before beginning, you will need to install various dependencies required for the
scripts. If using the `conda` package manager, this can be done by running these
commands:

```
conda create -n price_response_data -c conda-forge -c nrel scikit-learn=0.21.3 sqlalchemy=1.3.10 numpy=1.17.3 pandas=0.25.2 psycopg2=2.8.4 openpyxl=2.6.4 nrel-pysam=1.2.1 xlrd=1.2.0
conda activate price_response_data
```

If you are using Apple silicon (M1 or M2 processor), you will need to add
`CONDA_SUBDIR=osx-64 ` at the start of the first command above to use x86
versions of the software, since there is no arm64 version of PySAM 1.2.1. (It
may be possible to use a later version of PySAM, but that will change some of
the data clustering and there is still no PySAM package for arm64 as of Dec.
2022.)

You will also need to download some public data that are not stored in this
repository. See instructions in the following files:

- Loads/FERC Form 714 Database/data source.txt
- EV Adoption/NHTS 2017/data source.txt
- Resource Assessment/NSRDB Hourly Irradiance Data/download_nsrdb_data.py

You will also need to setup a postgresql database server, create an empty
database on it called 'switch' with timezone 'Pacific/Honolulu', and set
`db_host` in `build_database/util.py` to match the hostname for the database
server if it is not `'localhost'`).

The main script is `build_database/import_data.py`. This will pull data from all
the input files into the `switch` postgresql database. See
`build_database/import_data.py` for a description of the configuration files you
need to connect to the database.

Most of the inputs for `import_data.py` are included in this repository, but
some large input files need to be downloaded from public sources. If you get a
"file not found" error, look for a "data sources.txt" file, download script or
similar file in the relevant directory, and follow the instructions to download
those files. There are also some files titled `steps to create ... .txt` that
describe steps to follow with GIS software to perform land use screening. The
results from this screening are already stored in the repository for use by
`import_data.py`, so you do not need to re-run them. But you can review those
instructions to see exactly how the screening was done, and you can modify them
and follow the new instructions if you want to change the screening rules.

After the `switch` database is constructed, you can use
`switch_model.hawaii.scenario_data` (part of the main Switch software
distribution) to extract data for the particular dates and cost scenarios needed
to run an individual model. See `get_scenario_data.py` scripts in various model
repositories on https://github.com/switch-hawaii/ for examples of how to do
this.

These are some of the important input files used to create the data warehouse:

- `Generator Info/build_database/import_data.py`
  - imports all data into `switch_hawaii` data warehouse
- `build_database/solar_resources.py`
  - contains functions to calculate hourly performance of rooftop and
    utility-scale solar (called by `import_data.py`)
- `Generator Info/Existing Plant Data.xlsx`
  - data describing the capabilities of existing power plants
- `Generator Info/PSIP 2016-12 ATB 2019 generator data.xlsx`
  - data describing new renewable projects that could be developed, as well as
    HECO sales forecasts
- `EV Adoption/EV projections with buses.xlsx`
  - EV adoption projections; sources are cited in the workbook
- `EV Adoption/ev_hourly_charge_profile.tsv`
  - business-as-usual EV charging shapes from http://fsec.ucf.edu/en/publications/pdf/HI-14-17.pdf

The instructions above recreate the data environment used for historical studies
as closely as possible. However, two factors make it impossible to recreate the
data exactly:

(1) NREL updates the National Solar Radiation Database (NSRDB) periodically, so
    the data files you download from NSRDB are likely to differ slightly from the
    ones that we used in the past.
(2) We cluster distributed PV resources into tranches with similar performance.
    As of the time we prepared data for this study, the clustering method
    depended on random starting values, so the clustering will be slightly
    different each time new solar data are prepared.