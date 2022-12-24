#!/usr/bin/python
# -*- coding: utf-8 -*-

# sample SAM/NSRDB code available here:
# https://sam.nrel.gov/samples
# https://developer.nrel.gov/docs/solar/pvwatts-v5/
# https://nsrdb.nrel.gov/api-instructions
# https://sam.nrel.gov/sdk

# note: this script can be run piecemeal from iPython/Jupyter, or all-at-once from the command line

# NOTE: this uses pandas DataFrames for most calculations, and breaks the work into batches
# to avoid exceeding available memory (e.g., do one load zone worth of projects at a time,
# then calculate one grid cell at a time and add it to the relevant projects). However, this
# makes the code complex and the logic unclear, so it would probably be better to use a database,
# where everything can be calculated at once in one query. (e.g. get a list of grid cells for all
# projects from the database, then calculate cap factor for each grid cell and store it incrementally
# in a cell cap factor table, then run one query which joins project -> project cell -> cell to
# get hourly cap factors for all projects. This could use temporary tables for the cells, which
# are then discarded.

# NOTE: this stores all the hourly capacity factors in the postgresql database. That makes it
# difficult to share with others. An alternative would be to store a separate text file for each
# technology for each day and sync those via github. Disadvantages of that: querying is more complex
# and we have to specify a reference time zone before saving the data (day boundaries are time zone
# specific). Alternative: store in postgresql and publish a dump of the database.

from __future__ import print_function, division
from __future__ import absolute_import
import os, re, sys, struct, ctypes, datetime
from collections import OrderedDict
import numpy as np
import pandas as pd
import dateutil.tz     # should be available, since pandas uses it
import sqlalchemy

from k_means import KMeans
from util import execute, executemany, switch_db, pg_host, copy_dataframe_to_table
import shared_tables

# number of digits that latitude and longitude should be rounded to before matching
# to the nsrdb files
lat_lon_digits = 2
# location of main database directory relative to this script file
database_rel_dir = '../Resource Assessment'
# location of nsrdb hourly data files relative to the main database directory
# all subdirectories of this one will be scanned for data files
nsrdb_dir = 'NSRDB Hourly Irradiance Data'
# pattern to match lat and lon in nsrdb file name (all matching files will
# be read for that site, e.g., for multiple years); this should specify
# named groups for at least 'lat' and 'lon'.
# note: we don't try too hard to match an exact pattern of digits and symbols
# (just use .* for each group). If the expressions can't be parsed, we let them
# generate errors later.
nsrdb_file_regex = re.compile(r'^(?P<stn>.*)_(?P<lat>.*)_(?P<lon>.*)_(?P<year>.*)[.]csv$')

# load zone for which data is being prepared
# TODO: add load zone to cluster input file
# TODO: make tracking_pv code work with multiple load_zones
load_zone = 'Oahu'
load_zones = [load_zone]


# Set PV system parameters
# module_type: 0=standard, 1=premium, 2=thin film; we use a reasonable value (probably default)
# pre-inverter losses and inverter efficiency from NREL 2019 ATB, pp. 22, 28 and 38 of
# https://www.nrel.gov/docs/fy19osti/72399.pdf
# degradation is effect of 0.75%/year losses (from NREL 2019 ATB), averaged over 30 years
# (see "Generator Info/PSIP 2016-12 ATB 2019 generator data.xlsx")
std_pv_config = dict(
    module_type=0,
    inv_eff=98,
    losses=100 * (1 -
        (1 - 0.095)     # pre-inverter
        * (1 - 0.1015)  # average degradation over panel life
    )
)
def pv_settings(d):
    # add data from dict-like object d to std_pv_config, and return the resulting dict
    result = std_pv_config.copy()
    result.update(d)
    return result
# These system-specific settings will be assigned below:
# azimuth: degrees from north (0)
# tilt: in degrees from flat (0)
# array_type: 0=fixed rack, 1=fixed roof, 2=single-axis, 3=single-axis backtracked
# gcr: ground cover ratio (may be used for backtrack and shading calculations)
# dc_ac_ratio: DC (panel) rating vs. AC (inverter) rating of system

# def dict_add(d, **kwargs):
#     d = d.copy()
#     d.update(kwargs)
#     return d

# Available roof area calibrated to Project Sunroof data for Oahu:
# our roof inventory from Google map images gives 60208422 m2 in Oahu,
# [all_cells.reset_index().query("load_zone=='Oahu'")['roof_area'].sum()]
# and Google Sunroof reports 25991040 m2 of panel potential on Oahu
# So we assume that share (43.17%) of roof areas could have panels.
roof_panel_frac = 25991040. / 60208422.
# Within this, Project Sunroof reports roof directions as shown below (see
# "Switch-Hawaii/data/Resource Assessment/GIS/Rooftop PV/Project Sunroof/
# project sunroof Oahu slope analysis.ipynb"). The dataframe also shows
# the central angle for the azimuth quadrant (0=North), the assumed tilt
# of panels and the assumed ground cover ratio (gcr) for PVWatts. Note that
# gcr is only used to calculate shading, not coverage/capacity. This is
# appropriate since it is the percent coverage within the panel-covered regions,
# but isn't meant to account for other regions of the roof that have no panels.
# roof_panel_frac shows the percent coverage of all roof area.

# We assume panels on flat roofs are sloped at 5 degrees and panels on sloped
# roofs are tilted at 25 degrees, matching 2019 ATB
# (https://atb.nrel.gov/electricity/2019/index.html?t=sd and
# https://atb.nrel.gov/electricity/2019/index.html?t=sr)
# (can use np.nan here to assign latitude instead, probably need gcr=0.7 then).
# NREL 2019 ATB gets parameters from Fu et al. 2018 (https://www.nrel.gov/docs/fy19osti/72399.pdf).
# We assume a DC/AC ratio of 1.15, based on pp. 22 and 28 of Fu et al. 2018 and
# panel efficiency for commercial and residential systems from pp. 17 and 25 of Fu et al. 2018.

# (based on California systems installed in 2016), from pp. 28 and 19 of
# https://www.nrel.gov/docs/fy17osti/68925.pdf

configs = pd.DataFrame.from_records(
    [
        ('FlatDistPV',   'f', 0.406397, 0.191, 180, 0, 5,  0.9, 1.15),
        ('SlopedDistPV', 'n', 0.133257, 0.172, 0,   1, 25, 0.9, 1.15),
        ('SlopedDistPV', 'e', 0.143570, 0.172, 90,  1, 25, 0.9, 1.15),
        ('SlopedDistPV', 's', 0.163764, 0.172, 180, 1, 25, 0.9, 1.15),
        ('SlopedDistPV', 'w', 0.153012, 0.172, 270, 1, 25, 0.9, 1.15)
    ],
    columns=[
        'technology', 'orientation', 'roof_fraction', 'panel_efficiency',
        'azimuth', 'array_type', 'tilt', 'gcr', 'dc_ac_ratio'
    ]
).set_index('technology')

# We assume that panels within each quadrant are evenly distributed between the cardinal
# direction and +/- 30 degrees from that. (Oahu has street grids oriented in a variety of
# directions, with no clear preference for any particular azimuth.)
dist_pv_configs = pd.concat([configs] * 3).sort_values('orientation')
dist_pv_configs['azimuth'] = (
    dist_pv_configs['azimuth'] + np.tile([-30, 0, 30], configs.shape[0])
).mod(360)
# account for actual coverage and splitting each quadrant into 3 tranches
dist_pv_configs['roof_fraction'] = roof_panel_frac * dist_pv_configs['roof_fraction'] / 3
# convert SAM settings into a dictionary and drop other columns
dist_pv_configs['settings'] = \
    dist_pv_configs.loc[:, 'azimuth':'dc_ac_ratio'].apply(pv_settings, axis=1)
dist_pv_configs = \
    dist_pv_configs.loc[:, ['orientation', 'roof_fraction', 'panel_efficiency', 'settings']]

# Notes for utility-scale PV:
# TODO: calculate performance for each 10m cell separately, then aggregate
#       (allows use of exact slopes, but requires much more calculation;
#        better to use current approach, with 1% slope bands)
# TODO: use southward ground slope for tracking PV, latitude as slope for fixed PV
# TODO: calculate rooftop PV in all areas with roofs (similar to fixed PV, but
#       using roof area instead of suitable land, and more different slope bands;
#       flat-roof systems seem to be tilted to match latitude -
#        http://www.recsolar.com/commercial-solar-project-examples/)

# tuple of technology name and array_type for pvwatts
# note: Appendix F of 2016-04-01 PSIP uses 2 for tracking, but 3 (backtracking) seems like a better choice.
# note: p. v of http://www.nrel.gov/docs/fy13osti/56290.pdf says projects < 20 MW use 7.6/8.7 acres/MWac,
# but Waianae Solar project on Oahu uses 4/5 acres per MW(DC), which equates to 6/7.5 acres per MW(AC)
# with dc/ac ratio of 1.5 (they may be using less). See e-mail from M Fripp to Todd Kanja 12:47 2016-07-08 HST.
# These densities correspond roughly to the lower quartile of land-use intensity for small projects, shown on
# p. 11 of http://www.nrel.gov/docs/fy13osti/56290.pdf, so we also use the upper-quartile of the ground cover ratio
# (GCR, packing factor) for all PV projects, shown on p. 13.
# note: Appendix F uses 7.6/8.7 acres per MW(AC) (fixed/tracking)(?) and gcr=0.4, which seems less valid for Oahu.
# We assume a DC/AC ratio of 1.3, based on p. 38 of https://www.nrel.gov/docs/fy19osti/72399.pdf
# and https://atb.nrel.gov/electricity/2019/index.html?t=su
# Also note: Aloha Solar on Oahu uses 22 acres for a 5 MWac project (4.4 acres/MWac): https://dbedt.hawaii.gov/hcda/files/2017/12/KAL-17-017-ASEF-II-Development-Permit-Application.pdf
central_solar_techs = pd.DataFrame.from_records(
    [
        # ('CentralFixedPV',    6,   180, 0, np.nan, 0.68, 1.3), # not used so far, omit for speed
        ('CentralTrackingPV', 7.5, 180, 3, 0,      0.45, 1.3),
    ],
    columns=[
        'technology', 'acres_per_mw', 'azimuth', 'array_type', 'tilt', 'gcr', 'dc_ac_ratio'
    ]
)
# note: tilt=np.nan means use cell latitude

# 1 / [(m2/acre) * (acre/mw)]
central_solar_techs['mw_per_m2'] = (1.0 / (4046.86 * central_solar_techs['acres_per_mw']))
# convert SAM settings into a dictionary and drop other columns
central_solar_techs['settings'] = (
    central_solar_techs.loc[:, 'azimuth':'dc_ac_ratio'].apply(pv_settings, axis=1)
)
central_solar_techs = central_solar_techs.loc[:, ['technology', 'mw_per_m2', 'settings']]
central_solar_techs.set_index('technology', inplace=True)
#central_solar_techs.loc['CentralTrackingPV', 'settings']['gcr']

# find the database directory and System Advisor Model
try:
    curdir = os.path.dirname(__file__)
except NameError:
    # no __file__ variable; we're copying and pasting in an interactive session
    curdir = os.getcwd()
    pd.set_option('display.width', 200)

database_dir = os.path.normpath(os.path.join(curdir, database_rel_dir))
if not os.path.exists(database_dir):
    raise RuntimeError("Unable to find database directory at " + database_dir)


def module_setup():
    # run code that is needed to setup the module
    # We call it from here so it can be at the top of the file, but still executed
    # after all the functions below are defined. It can't be called from main()
    # because sometimes functions from this module are imported into other scripts, and
    # main() is never called.

    # setup database
    global db_engine
    db_engine = sqlalchemy.create_engine('postgresql://' + pg_host + '/' + switch_db)

    # make lists of all available NSRDB data files
    # and years to accumulate data for
    global nsrdb_file_dict, years
    nsrdb_file_dict, years = get_nsrdb_file_dict()
    # years = [2007, 2008]

    # load System Advisor Model components
    global ssc, pvwatts5
    ssc, pvwatts5 = load_sam()


def main():
    tracking_pv()
    distributed_pv()


def load_sam():
    # # location of System Advisor Model SDK relative to this script file
    # sam_sdk_rel_dir = 'System Advisor Model'
    # sam_sdk_dir = os.path.normpath(os.path.join(curdir, sam_sdk_rel_dir))
    # if not os.path.exists(sam_sdk_dir):
    #     raise RuntimeError("Unable to find System Advisor Model (SAM) SDK directory at " + sam_sdk_dir)

    # # Load the System Advisor Model (SAM) SDK API
    # # Note: SAM SDK can be downloaded from https://sam.nrel.gov/sdk
    # # nsrdb/sam code is based on examples in sscapi.py itself
    # # Also see https://nsrdb.nrel.gov/api-instructions and sam-sdk/ssc_guide.pdf
    #
    # # preload ssc library, so sscapi won't fail if it's not in the library search path
    # if sys.platform == 'win32' or sys.platform == 'cygwin':
    #     if 8 * struct.calcsize("P") == 64:
    #         path = ['win64', 'ssc.dll']
    #     else:
    #         path = ['win32', 'ssc.dll']
    # elif sys.platform == 'darwin':
    #     path = ['osx64', 'ssc.dylib']
    # elif sys.platform == 'linux2':
    #     path = ['linux64', 'ssc.so']
    # else:
    #     raise RuntimeError('Unsupported operating system: {}'.format(sys.platform))
    # ssc_dll = ctypes.CDLL(os.path.join(sam_sdk_dir, *path))
    #
    # # add search path to sscapi.py
    # sys.path.append(os.path.join(sam_sdk_dir, 'languages', 'python'))
    # import sscapi
    # ssc = sscapi.PySSC()
    # pvwatts5 = ssc.module_create("pvwattsv5")
    # ssc.module_exec_set_print(0)

    # note: PySAM (requires Python 3.5+) is available via
    # conda install -c nrel nrel-pysam
    # or pip install nrel-pysam
    from PySAM.PySSC import PySSC
    ssc = PySSC()
    pvwatts5 = ssc.module_create(b"pvwattsv5")
    ssc.module_exec_set_print(0)

    return ssc, pvwatts5


def tracking_pv():
    # note: this uses global nsrdb_file_dict and years variables

    cluster_cell = pd.read_csv(
        db_path('GIS/Utility-Scale Solar Sites/solar_cluster_nsrdb_grid_final.csv'),
        index_col='gridclstid'
    )
    cluster_cell = cluster_cell[cluster_cell['solar_covg']>0]
    cell = cluster_cell.groupby('nsrdb_id')
    cluster = cluster_cell.groupby('cluster_id')
    cluster_total_solar_area = cluster['solar_area'].sum()

    cluster_ids = list(cluster.groups.keys())             # list of all cluster ids, for convenience
    cluster_id_digits = len(str(max(cluster_ids)))  # max number of digits for a cluster id
    # site_ids for each cluster_id (these distinguish PV sites from wind sites that may have the same number)
    site_ids = ['PV_' + str(cluster_id).zfill(cluster_id_digits) for cluster_id in cluster_ids]

    # calculate weighted average lat and lon for each cluster
    # (note: the axis=0 and axis=1 keep pandas from generating lots of nans due to
    # trying to match column name in addition to row index)
    cluster_coords = pd.concat([
        cluster_cell['cluster_id'],
        cluster_cell[['solar_lat', 'solar_lon']].multiply(cluster_cell['solar_area'], axis=0)
    ], axis=1).groupby('cluster_id').sum().div(cluster_total_solar_area, axis=0)
    cluster_coords.columns=['latitude', 'longitude']

    # get list of technologies to be defined
    technologies = central_solar_techs.index.values

    # calculate capacity factors for all projects

    # This dict will hold vectors of capacity factors for each cluster for each year and technology.
    # This arrangement is simpler than using a DataFrame because we don't yet know the
    # indexes (timesteps) of the data for each year.
    cluster_cap_factors = dict()
    for tech in technologies:
        # go through all the needed nsrdb cells and add them to the capacity factor for the
        # relevant cluster and year
        print("Calculating capacity factors for {} cells (abt 1 min.).".format(tech))
        for i, (cell_id, grp) in enumerate(cell):
            # print('{}/{}'.format(i, len(cell)))
            # grp has one row for each cluster that uses data from this cell
            lat = round_coord(grp['nsrdb_lat'].iloc[0])
            lon = round_coord(grp['nsrdb_lon'].iloc[0])
            settings = central_solar_techs.loc[tech, 'settings'].copy()
            if np.isnan(settings['tilt']):
                # nan flag set for tilt; use latitude
                settings['tilt'] = lat

            for year in years:
                cap_factors = get_cap_factors(nsrdb_file_dict[lat, lon, year], settings)
                # note: iterrows() would convert everything to a single (float) series, but itertuples doesn't
                for clst in grp.itertuples():
                    contrib = cap_factors * clst.solar_area / cluster_total_solar_area[clst.cluster_id]
                    key = (tech, clst.cluster_id, year)
                    if key in cluster_cap_factors:
                        cluster_cap_factors[key] += contrib
                    else:
                        cluster_cap_factors[key] = contrib

    # get timesteps for each year (based on lat and lon of first cell in the list)
    timesteps = dict()
    lat = round_coord(cluster_cell['nsrdb_lat'].iloc[0])
    lon = round_coord(cluster_cell['nsrdb_lon'].iloc[0])
    for year in years:
        timesteps[year] = get_timesteps(nsrdb_file_dict[(lat, lon, year)])

    # make an index of all timesteps
    timestep_index = pd.concat([pd.DataFrame(index=x) for x in timesteps.values()]).index.sort_values()

    # make a single dataframe to hold all the data
    cap_factor_df = pd.DataFrame(
        index=timestep_index,
        columns=pd.MultiIndex.from_product([technologies, site_ids]),
        dtype=float
    )

    # assign values to the dataframe
    for ((tech, cluster_id, year), cap_factors) in cluster_cap_factors.items():
        cap_factor_df.update(pd.DataFrame(
                cap_factors,
                index=timesteps[year],
                columns=[(tech, 'PV_' + str(cluster_id).zfill(cluster_id_digits))]
        ))
    cap_factor_df.columns.names = ['technology', 'site']
    cap_factor_df.index.names=['date_time']

    # add load_zone and orientation to the index
    cap_factor_df['load_zone'] = load_zone
    cap_factor_df['orientation'] = 'na'
    cap_factor_df.set_index(['load_zone', 'orientation'], append=True, inplace=True)
    # convert to database orientation, with natural order for indexes,
    # but also keep as a DataFrame
    cap_factor_df = pd.DataFrame(
        {'cap_factor': cap_factor_df.stack(cap_factor_df.columns.names)}
    )
    # sort table, then switch to using z, t, s, o as index (to match with project table)
    cap_factor_df = cap_factor_df.reorder_levels(
        ['load_zone', 'technology', 'site', 'orientation', 'date_time']
    ).sort_index().reset_index('date_time')

    # make a dataframe showing potential projects (same structure as "project" table)
    # note: for now we don't really handle multiple load zones and we don't worry about orientation
    # (may eventually have projects available with different azimuth and slope)
    # This concatenates a list of DataFrames, one for each technology
    project_df = pd.concat([
        pd.DataFrame(dict(
            load_zone=load_zone,
            technology=tech,
            site=site_ids,
            orientation='na',
            max_capacity=cluster_total_solar_area*central_solar_techs.loc[tech, 'mw_per_m2'],
            latitude=cluster_coords['latitude'],
            longitude=cluster_coords['longitude'],
        ))
        for tech in technologies
    ], axis=0).set_index(['load_zone', 'technology', 'site', 'orientation'])


    # store data in postgresql tables
    shared_tables.create_table("project")
    execute("DELETE FROM project WHERE technology IN %s;", [tuple(technologies)])
    project_df.to_sql('project', db_engine, if_exists='append')

    # retrieve the project IDs (created automatically in the database)
    project_ids = pd.read_sql(
        "SELECT project_id, load_zone, technology, site, orientation "
        + "FROM project WHERE technology IN %(techs)s;",
        db_engine, index_col=['load_zone', 'technology', 'site', 'orientation'],
        params={'techs': tuple(technologies)}
    )
    cap_factor_df['project_id'] = project_ids['project_id']

    # convert date_time values into strings for insertion into postgresql.
    # Inserting a timezone-aware DatetimeIndex into postgresql fails; see
    # http://stackoverflow.com/questions/35435424/pandas-to-sql-gives-valueerror-with-timezone-aware-column/35552061
    # note: the string conversion is pretty slow
    cap_factor_df['date_time'] = pd.DatetimeIndex(cap_factor_df['date_time']).strftime("%Y-%m-%d %H:%M:%S%z")

    # Do we need error checking here? If any projects aren't in cap_factor_df, they'll
    # create single rows with NaNs (and any prior existing cap_factors for them will
    # get dropped below).
    # If any rows in cap_factor_df aren't matched to a project, they'll go in with
    # a null project_id.

    shared_tables.create_table("cap_factor")    # only created if it doesn't exist
    shared_tables.drop_indexes("cap_factor")    # drop and recreate is faster than incremental sorting
    # drop any rows that no longer have records in projects table
    execute("DELETE FROM cap_factor c WHERE NOT EXISTS (SELECT * FROM project p WHERE p.project_id=c.project_id);")
    # cap_factor_df.to_sql('cap_factor', db_engine, if_exists='append', chunksize=10000)
    copy_dataframe_to_table(cap_factor_df, 'cap_factor')
    shared_tables.create_indexes("cap_factor")


def distributed_pv():
    """
    add records to database for each distributed PV technology, grouping into
    an appropriate number of tranches
    """

    # make sure tables exist, and clear out existing DistPV data;
    # the functions below will add records back to these tables
    shared_tables.create_table("project")
    shared_tables.create_table("cap_factor")
    # drop and recreate is faster than incremental sorting
    shared_tables.drop_indexes("cap_factor")
    execute("""
        DELETE FROM cap_factor c
            USING project p
            WHERE c.project_id=p.project_id AND p.technology in %(pv_techs)s;
        DELETE FROM project
            WHERE technology in %(pv_techs)s;
        DROP TABLE IF EXISTS dist_pv_details;
    """, {'pv_techs': ('DistPV', 'SlopedDistPV', 'FlatDistPV')})

    add_distributed_pv('FlatDistPV', 4)
    add_distributed_pv('SlopedDistPV', 16)

    # restore indexes, final cleanup
    shared_tables.create_indexes("cap_factor")
    # execute("ALTER TABLE dist_pv_details OWNER TO admin;")


def add_distributed_pv(technology, n_tranches):
    # TODO: break up the major sub-sections of the main loop into separate functions
    # TODO: merge the code that gets capacity factors for each configuration here
    # with the code that gets capacity factors for each cell for utility-scale PV.
    # TODO: write a general function that adds a block of projects and capacity
    # factors to the postgresql database (including reading back the project IDs),
    # and use that for distributed PV, utility-scale PV and wind projects

    # technology, n_tranches = 'FlatDistPV', 4
    # technology, n_tranches = 'SlopedDistPV', 16

    # read roof areas from load_zone_grid_cell.csv
    # treat any NaNs (missing data) as 0.0 coverage
    # See "Resource Assessment/GIS/Rooftop PV/steps to produce rooftop solar files.txt"
    # for steps to create this file.
    all_cells = pd.read_csv(
        db_path('GIS/General/load_zone_nsrdb_cell.csv'),
        index_col=['load_zone', 'nsrdb_id']
    ).fillna(0.0)

    # calculate hourly capacity factor for all dist pv configurations
    # for each cell in each load zone
    print("Calculating hourly capacity factor for all {} configurations (abt. 1-3 mins).".format(technology))
    for lz in load_zones:
        # lz = 'Oahu'
        lz_cells = all_cells.loc[lz, :]
        lz_cells = lz_cells[lz_cells.roof_area > 0.0]
        # create an array to hold hourly capacity factors for all cells in this load zone
        # it will end up with one row for each cell and one column for each hour
        cap_factors = None
        for cell_n, cell in enumerate(lz_cells.itertuples()):
            cell_capacities, cell_cap_factors = get_dist_pv_cap_factors(
                technology, cell.nsrdb_lat, cell.nsrdb_lon, cell.roof_area
            )
            # note: this is the first time when we know how many configurations
            # and timesteps there are, so this is when we create the cap_factors array
            if cap_factors is None:
                capacities = np.empty((len(lz_cells),) + cell_capacities.shape)
                cap_factors = np.empty((len(lz_cells),) + cell_cap_factors.shape)
                # fill them with nans, so we'll see if any aren't filled later
                capacities.fill(np.nan)
                cap_factors.fill(np.nan)
            capacities[cell_n, :] = cell_capacities
            cap_factors[cell_n, :, :] = cell_cap_factors

        # reshape into a long list of resources instead of a cell x config matrix
        capacities = capacities.reshape((-1,))
        cap_factors = cap_factors.reshape((-1, cap_factors.shape[2]))


        ##################
        # Bypass clustering if wanted (alternative to block below)
        # cluster_capacities = capacities
        # cluster_cap_factors = cap_factors.T
        # cluster_ids = np.arange(len(cluster_capacities))

        ##################
        # cluster available resources into tranches with similar timing and quality
        # (we assume the better-suited ones will be developed before the worse ones)
        # (This could be sped up by using a subsample of the timesteps if needed, but then
        # the cluster means would have to be calculated afterwards.)
        # an alternative approach would be to cluster resources based on annual average
        # capacity factor, but that neglects differences in timing between different
        # orientations.
        print(
            "Performing k-means clustering on {} configurations: initializing centers (abt. 1-3 mins)."
            .format(technology)
        )
        km = KMeans(n_tranches, X=cap_factors, size=capacities)
        import time
        start = time.time()
        km.init_centers()
        km.__dict__
        print("init_centers(): {} s".format(time.time()-start))
        start = time.time()
        print(
            "Performing k-means clustering on {} configurations: finding centers (3-6 mins)."
            .format(technology)
        )
        km.find_centers()
        print("find_centers(): {} s".format(time.time()-start))
        # now km.mu is a matrix of capacity factors, with one row per cluster
        # and one column per timestep
        # and km.cluster_id shows which cluster each resource belongs to

        cluster_capacities = np.bincount(km.cluster_id, weights=capacities)
        cluster_cap_factors = km.mu.T
        cluster_ids = km.cluster_id
        ##################


        # PROJECT TABLE

        # store project definitions and capacity factors
        print("Saving {} project definitions to database.".format(technology))
        project_df = pd.DataFrame(
            OrderedDict([
                ('load_zone', load_zone),
                ('technology', technology),
                ('site', [
                    'Oahu_{}_{}'.format(technology, i)
                    for i in range(len(cluster_capacities))
                ]),
                ('orientation', 'na'),
                ('max_capacity', cluster_capacities)
            ]),
        ).set_index(['load_zone', 'technology', 'site', 'orientation'])
        project_df.to_sql('project', db_engine, if_exists='append')

        # CAP_FACTOR TABLE

        print("Linking capacity factor table with database records.")
        # get timesteps for each year (based on lat and lon of last cell in the list)
        timesteps = [
            get_timesteps(nsrdb_file_dict[(cell.nsrdb_lat, cell.nsrdb_lon, year)])
                for year in years
        ]
        # make an index of all timesteps
        timestep_index = pd.concat(
            (pd.DataFrame(index=x) for x in timesteps)
        ).index.sort_values()

        # make an index of all site_ids
        # TODO: change this code and project_df code to zero-fill site numbers up to 2 digits
        # (enough to cover the number of tranches in each zone)
        site_ids = [
            '_'.join([load_zone, technology, str(i)])
                for i in range(cluster_cap_factors.shape[1])
        ]

        # multiindex of load_zone, technology, site, orientation
        proj_index = pd.MultiIndex.from_product([
            [load_zone], [technology], site_ids, ['na']
        ])

        # make a single dataframe to hold all the data
        cap_factor_df = pd.DataFrame(
            cluster_cap_factors,
            index=timestep_index,
            columns=proj_index,
        )
        cap_factor_df.columns.names = ['load_zone', 'technology', 'site', 'orientation']
        cap_factor_df.index.names=['date_time']

        # convert to database orientation, with natural order for indexes,
        # but also keep as a DataFrame
        cap_factor_df = pd.DataFrame(
            {'cap_factor': cap_factor_df.stack(cap_factor_df.columns.names)}
        )
        # sort table, then switch to using z, t, s, o as index (to match with project table)
        # (takes a few minutes)
        cap_factor_df = cap_factor_df.reorder_levels(
            ['load_zone', 'technology', 'site', 'orientation', 'date_time']
        ).sort_index().reset_index('date_time')

        # retrieve the project IDs (created automatically in the database earlier)
        # note: this read-back could potentially be done earlier, and then the
        # project ids could be included in cap_factor_df when it is first created.
        # but this provides cross-referencing by z, t, s, o automatically, which is helpful.
        project_ids = pd.read_sql(
            "SELECT project_id, load_zone, technology, site, orientation "
            "FROM project WHERE technology = '{}';".format(technology),
            db_engine,
            index_col=['load_zone', 'technology', 'site', 'orientation']
        )
        cap_factor_df['project_id'] = project_ids['project_id']

        # convert date_time values into strings for insertion into postgresql.
        # Inserting a timezone-aware DatetimeIndex into postgresql fails; see
        # http://stackoverflow.com/questions/35435424/pandas-to-sql-gives-valueerror-with-timezone-aware-column/35552061
        # note: the string conversion is pretty slow
        cap_factor_df['date_time'] = \
            pd.DatetimeIndex(cap_factor_df['date_time']).strftime("%Y-%m-%d %H:%M:%S%z")

        cap_factor_df.set_index(['project_id', 'date_time'], inplace=True)
        # Do we need error checking here? If any projects aren't in cap_factor_df, they'll
        # create single rows with NaNs (and any prior existing cap_factors for them will
        # get dropped below).
        # If any rows in cap_factor_df aren't matched to a project, they'll go in with
        # a null project_id.

        print("Saving {} capacity factors to database.".format(technology))
        # The next line is very slow. But it only seems possible to speed it up by
        # copying the data to a csv and doing a bulk insert, which is more cumbersome.
        # progress can be monitored via this command in psql:
        # select query from pg_stat_activity where query like 'INSERT%';
        # cap_factor_df.to_sql('cap_factor', db_engine, if_exists='append', chunksize=10000)
        copy_dataframe_to_table(cap_factor_df.reset_index(), 'cap_factor')

        # DIST_PV_DETAILS TABLE (optional)

        print("Saving {} cluster data to database.".format(technology))
        # store cluster details for later reference
        # would be interesting to see mean and stdev of lat, lon,
        # cap factor, azimuth, tilt for each cluster, so we can describe them.

        # use descriptive data for each cell and each configuration as
        # row and column indexes (will later be saved along with data)
        rows = pd.MultiIndex.from_arrays(
            lz_cells[col] for col in ['nsrdb_lat', 'nsrdb_lon']
        )
        cols = pd.MultiIndex.from_arrays(
            dist_pv_configs.loc[technology, col].astype(str)
            for col in dist_pv_configs.columns
        )
        data = {
            'capacity_mw': capacities.reshape((len(lz_cells), -1)),
            'tranche': (
                'Oahu_'
                + technology + '_'
                + cluster_ids.astype(str).astype(np.object)
            ).reshape((len(lz_cells), -1))
        }
        key, vals = list(data.items())[0]
        dist_pv_details = pd.concat(
            [
                pd.DataFrame(vals, index=rows, columns=cols).stack(cols.names).to_frame(name=key)
                for key, vals in data.items()
            ],
            axis=1
        ).reset_index()
        dist_pv_details.insert(0, 'technology', technology)
        dist_pv_details.insert(0, 'load_zone', load_zone)

        # store in postgresql database
        dist_pv_details.to_sql('dist_pv_details', db_engine, if_exists='append')


def get_cap_factors(file, settings):
    # file, settings = nsrdb_file_dict[lat, lon, year], settings
    dat = ssc.data_create()

    # make sure all needed settings have been specified (there may be other optional ones too)
    assert(all(k in settings for k in [
        'azimuth', 'tilt', 'array_type', 'gcr', 'dc_ac_ratio', 'inv_eff',
        'losses', 'module_type'
    ]))
    assert(not np.isnan(settings['tilt'])) # make sure none got through

    # assign standard settings (also convert keys from str to bytes)
    for key, val in settings.items():
        ssc.data_set_number(dat, key.encode(), val)

    # dc rating in kW for a 1 kW AC system
    ssc.data_set_number(dat, b'system_capacity', settings['dc_ac_ratio'])
    ssc.data_set_number(dat, b'adjust:constant', 0) # required

    # specify the file holding the solar data
    ssc.data_set_string(dat, b'solar_resource_file', file.encode())

    # run PVWatts5
    if ssc.module_exec(pvwatts5, dat) == 0:
        err = b'PVWatts V5 simulation error:\n'
        idx = 1
        msg = ssc.module_log(pvwatts5, 0)
        while (msg is not None):
            err += '\t: {}\n'.format(msg)
            msg = ssc.module_log(pvwatts5, idx)
            idx += 1
        raise RuntimeError(err.strip())
    else:
        # get power production in kW; for a 1 kW AC system this is also the capacity factor
        cap_factors = np.asarray(ssc.data_get_array(dat, b'gen'), dtype=float)

    ssc.data_free(dat)

    return cap_factors

def get_timesteps(file):
    """
    Retrieve timesteps from nsrdb file as pandas datetime series.
    Based on code in sscapi.run_test2().
    """
    dat = ssc.data_create()

    ssc.data_set_string(dat, b'file_name', file.encode())
    ssc.module_exec_simple_no_thread(b'wfreader', dat)

    # create a tzinfo structure for this file
    # note: nsrdb uses a fixed offset from UTC, i.e., no daylight saving time
    tz_offset = ssc.data_get_number(dat, b'tz')
    tzinfo = dateutil.tz.tzoffset(None, 3600 * tz_offset)

    df = pd.DataFrame(dict(
        year=ssc.data_get_array(dat, b'year'),
        month=ssc.data_get_array(dat, b'month'),
        day=ssc.data_get_array(dat, b'day'),
        hour=ssc.data_get_array(dat, b'hour'),
        minute=ssc.data_get_array(dat, b'minute'),
    )).astype(int)

    ssc.data_free(dat)

    # create pandas DatetimeIndex for the timesteps in the file
    # note: we ignore minutes because time indexes always refer to the start of the hour
    # in our database
    # note: if you use tz-aware datetime objects with pd.DatetimeIndex(), it converts them
    # to UTC and makes them tz-naive. If you use pd.to_datetime() to make a column of datetime
    # values, you have to specify UTC=True and then it does the same thing.
    # So we start with naive datetimes and then specify the tzinfo when creating the
    # DatetimeIndex. (We could also use idx.localize(tzinfo) after creating a naive DatetimeIndex.)

    timesteps = pd.DatetimeIndex(
        [datetime.datetime(year=t.year, month=t.month, day=t.day, hour=t.hour) for t in df.itertuples()],
        tz=tzinfo
    )
    return timesteps

def get_dist_pv_cap_factors(technology, lat, lon, area):
    """
    Calculate size and capacity factors for all distributed PV configurations
    that have been defined with the specified technology label, if sited at the
    specified latitude and longitude, with the specified amount of total roof
    area.

    Returns capacity, cap_factors, where capacity is a vector of capacities (in
    MW) available in each configuration, and cap_factors is a matrix showing
    hourly capacity factors for each configuration. cap_factors has one row per
    configuration and one column per timestep. area should be total roof area in
    the cell, in square meters.

    Note: area * dist_pv_configs['roof_fraction'] should be total roof area
    covered by panels in m^2, allowing for non-covered areas and space between
    panels. dist_pv_configs['settings']['gcr'] is the percent coverage within
    the panel-covered regions, allowing for space between panels; it only affects
    shading calculations, not capacity.
    """
    # technology, lat, lon, area = technology, cell.nsrdb_lat, cell.nsrdb_lon, cell.roof_area
    capacities = []
    cap_factors = []
    for config in dist_pv_configs.loc[technology, :].itertuples():
        # capacity in MW, assuming 1000 W/m2 of irradiance (0.001 MW/m2)
        settings = config.settings.copy()
        if np.isnan(settings['tilt']):
            # flat roof, use latitude
            settings['tilt'] = lat
        capacities.append(
            0.001 * area * config.roof_fraction   # MW of irradiance under rated conditions
            * config.panel_efficiency             # DC output at rated conditions, i.e., DC rating
            * (1 / settings['dc_ac_ratio'])       # convert to AC rating
        )
        yearly_cfs = [
            get_cap_factors(nsrdb_file_dict[lat, lon, year], settings)
            for year in years
        ]
        # convert list of yearly capacity factors into a single vector
        # and add it to the list of configurations
        cap_factors.append(np.concatenate(yearly_cfs))
    return np.array(capacities), np.array(cap_factors)

def db_path(path):
    """Convert the path specified relative to the database directory into a real path.
    For convenience, this also converts '/' file separators to whatever is appropriate for
    the current operating system."""
    return os.path.join(database_dir, *path.split('/'))
def round_coord(coord):
    """convert lat or lon from whatever form it's currently in to a standard form (2-digit rounded float)
    this ensures stable matching in dictionaries, indexes, etc."""
    return round(float(coord), 2)

def get_nsrdb_file_dict():
    """get a list of all the files that have data for each lat/lon pair (parsed from the file names)"""
    file_dict = dict()
    years = set()
    for dir_name, dirs, files in os.walk(db_path(nsrdb_dir)):
        for f in files:
            file_path = os.path.join(dir_name, f)
            m = nsrdb_file_regex.match(f)
            if m is None:
                # print("Skipping unrecognized file {}".format(file_path))
                pass
            else:
                lat = round_coord(m.group('lat'))
                lon = round_coord(m.group('lon'))
                year = int(m.group('year'))
                file_dict[lat, lon, year] = file_path
                years.add(year)
    if not file_dict:
        print("WARNING: unable to find any NSRDB files in {}".format(db_path(nsrdb_dir)))
    return file_dict, years

# call standard module setup code (whether this is loaded as main or not)
module_setup()

# if __name__ == '__main__':
#     main()
