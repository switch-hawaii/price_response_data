#!/usr/bin/env python

# construct postgresql backend database for SWITCH-Hawaii.
# Data is pulled into this database from various sources (Excel files,
# GIS results, NSRDB/OWITS data files), and then switch_mod.hawaii.scenario_data
# can be used to construct any scenario using the accumulated data.

from __future__ import division, print_function, absolute_import

import sys, csv, datetime, os, collections, math, textwrap
import numpy as np
import pandas as pd
import sklearn.cluster, sklearn.metrics
import sqlalchemy
import shared_tables, solar_resources, util
from util import execute, executemany, pg_host, switch_db, copy_dataframe_to_table

try:
    import openpyxl
except ImportError:
    print("This script requires the openpyxl module to access the data in Microsoft Excel files.")
    print("Please execute 'conda install openpyxl' or 'pip install openpyxl'.")
    raise

db_engine = sqlalchemy.create_engine('postgresql://' + pg_host + '/' + switch_db)

# raise SettingWithCopyError instead of SettingWithCopyWarning
pd.set_option('mode.chained_assignment', 'raise')

# data files used by multiple functions
data_directory = '..'
# This spreadsheet is based on HECO's 2016-12-23 PSIP assumptions,
# stored in a separate spreadsheet on 2016-06-16. These assumptions were also used for
# 2016-09-07 workplan and update.
# technology_data_file = os.path.join(
#     data_directory, 'Generator Info', 'PSIP 2016-12 ATB 2018 Bloomberg generator data.xlsx'
# )
technology_data_file = os.path.join(
    data_directory, 'Generator Info', 'PSIP 2016-12 ATB 2019 generator data.xlsx'
)
# definitions to use for a flat-costs scenario
# (should we define this scenario in the xlsx file itself?)
flat_base_scenario = 'ATB_2019_mid'
flat_scenario = 'ATB_2019_flat'
flat_ref_year = 2019
# load scenario ID corresponding to this file
load_scen_id = 'PSIP_2016_12'

ferc714_load_file = os.path.join(
    data_directory, 'Loads', 'FERC Form 714 Database',
    'Part 3 Schedule 2 - Planning Area Hourly Demand.csv'
)
ferc714_respondent_file = os.path.join(
    data_directory, 'Loads', 'FERC Form 714 Database',
    'Part 1 Schedule 1 - Identification Certification.csv'
)

# technologies that are added to the project and cap_factor tables by
# onshore_wind(), offshore_wind(), tracking_pv.tracking_pv() and tracking_pv.distributed_pv(),
# not by generator_info
renewable_techs = ['SlopedDistPV', 'FlatDistPV', 'CentralTrackingPV', 'CentralFixedPV', 'OnshoreWind', 'OffshoreWind']
# note: generator_info() creates all technologies (in generator_info and generator_costs_by_year
# tables) and adds only the non-resource-limited technologies to the project table. The renewable
# projects link to generator_info via technology. So the renewable energy functions
# can (and should) be run before new_generator_info and existing_generator_info.
# note: ev_adoption is needed even when using ev_adoption_advanced(), because it creates
# the ev_adoption table with annual ev fleet shares for each load zone

# NOTE: before running this for the first time, you must run these commands in psql:
# DROP DATABASE switch; -- if it exists already
# CREATE DATABASE switch; -- should match util.switch_db
# ALTER DATABASE switch SET TIMEZONE TO 'Pacific/Honolulu';
def main():
    # run all the import scripts (or at least the ones we want)
    load_zones()
    ev_adoption()
    ev_adoption_advanced()  # also need to call ev_adoption() to get ev fleet shares
    fuel_costs()
    # fuel_costs_no_biofuel()  # obsolete
    energy_source_properties()
    system_load()
    interconnects()

    # renewable energy functions create the project and capacity factor tables
    # and fill them with data on all available renewable energy projects (new
    # and existing). These show what is available physically, regardless of
    # technology scenario. These shouldn't be re-run often, because they
    # re-cluster the solar (and wind?) sites each time.
    # If re-running, you should delete or rename project and cap_factor tables
    # at this point, to avoid carrying over old records. (proj_existing_builds,
    # generator_info and generator_costs_by_year will all be rebuilt from
    # scratch.)
    # execute("drop table project;")
    # execute("drop table cap_factor;")
    # or:
    # shared_tables.drop_indexes("project") # index names must be unique across tables(!)
    # execute("alter table project rename to project_2019_10_09;")
    # shared_tables.drop_indexes("cap_factor") # index names must be unique across tables(!)
    # execute("alter table cap_factor rename to cap_factor_2019_10_09;")
    solar_resources.tracking_pv()
    solar_resources.distributed_pv()
    onshore_wind()
    offshore_wind()
    renewable_supply_curve()
    # generator info functions create or replace tables with descriptions of all
    # renewable and fossil technologies (generator_info,
    # generator_costs_by_year, etc.). They also add records for fossil projects
    # and existing renewable projects to the projects table created earlier.
    # This can be re-run without re-running the renewable functions, because
    # it deletes all non-renewable records from the projects table and leaves
    # the renewable records.
    generator_info()
    # add interconnect costs to projects table; these aren't calculated earlier
    calculate_interconnect_costs()

    # various timeseries
    execute("""
        drop table if exists study_periods;
        drop table if exists study_date;
        drop table if exists study_hour;
    """)
    shared_tables.create_table('study_periods')
    shared_tables.create_table('study_date')
    shared_tables.create_table('study_hour')
    # rps_timeseries() # obsolete
    monthly_timeseries()
    daily_timeseries()
    long_slice_timeseries()
    pha_timeseries()
    # k_means_timeseries()
    k_means_timeseries_daily_avg()
    short_slice_timeseries()

def show_index(index):
    # useful for listing column names or index labels for a pandas dataframe
    # when working interactively
    print(textwrap.fill(', '.join(map(str, index)), 80))

def show_all(df):
    # useful for showing full dataframe when working interactively
    from IPython.display import display
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        display(df)

def data_dir(*path):
    return os.path.join(data_directory, *path)

def get_workbook(xlsx_file):
    return openpyxl.load_workbook(xlsx_file, data_only=True, read_only=True)

def get_table_from_xlsx(xlsx_file, named_range, transpose=False):
    data = [
        tuple(c.value for c in r)
        for r in get_named_region(xlsx_file, named_range)
    ]
    if transpose:
        data = list(zip(*data))
    head = data.pop(0)  # take out the header row
    data = list(zip(*data))   # switch from row to column orientation
    # make a dictionary, with one column for each element of header row
    return dict(zip(head, data))

def get_named_region(xlsx_file, named_range):
    # get a single rectangular region from the specified file in the source data directory
    wb = get_workbook(xlsx_file)
    if hasattr(wb, 'defined_names'):
        # new version of openpyxl
        try:
            full_range = wb.defined_names[named_range]
        except KeyError:
            raise ValueError(
                'Range "{}" not found in workbook "{}".'.format(named_range, xlsx_file))
        # full_range.destinations is a generator returning strings
        dests = list(full_range.destinations)
        if len(dests) > 1:
            raise ValueError(
                'Range "{}" in workbook "{}" contains more than one region.'
                .format(named_range, xlsx_file)
            )
        else:
            ws, region = dests[0]
            return wb[ws][region]
    else:
        # older version of openpyxl
        full_range = wb.get_named_range(named_range)
        if full_range is None:
            raise ValueError(
                'Range "{}" not found in workbook "{}".'.format(named_range, xlsx_file))
        # full_range.destinations is list or tuple returning worksheet objects and region names
        if len(full_range.destinations) > 1:
            raise ValueError(
                'Range "{}" in workbook "{}" contains more than one region.'
                .format(named_range, xlsx_file)
            )
        else:
            ws, region = full_range.destinations[0]
            return ws[region]

def data_frame_from_xlsx(xlsx_file, named_range):
    region = get_named_region(xlsx_file, named_range)
    return pd.DataFrame([cell.value for cell in row] for row in region)

def get_named_cell_from_xlsx(xlsx_file, named_range):
    region = get_named_region(xlsx_file, named_range)
    if isinstance(region, collections.abc.Iterable):
        raise ValueError(
            'Range "{}" in workbook "{}" does not refer to an individual cell.'.format(
                named_range, xlsx_file))
    return region.value

#########################
# rps timeseries (reusing version from before the server crashed)
def rps_timeseries():
    execute("""
        delete from study_date where time_sample='rps';
        delete from study_hour where time_sample='rps';
    """)

    with open('timeseries_rps.tab','r') as f:
        for r in csv.DictReader(f, delimiter='\t'):
            dt = str(r["TIMESERIES"])[2:8]
            execute("""
                INSERT INTO study_date
                    (time_sample, period, study_date,
                    month_of_year, date,
                    hours_in_sample,
                    ts_num_tps, ts_duration_of_tp, ts_scale_to_period)
                VALUES
                    (%s, %s, %s,
                    %s, %s,
                    %s,
                    %s, %s, %s);
            """,
                ('rps', r["ts_period"], r["TIMESERIES"],
                int(dt[2:4]), "20"+dt[0:2]+"-"+dt[2:4]+"-"+dt[4:6],
                float(r["ts_duration_of_tp"])*float(r["ts_scale_to_period"]),
                r["ts_num_tps"], r["ts_duration_of_tp"], r["ts_scale_to_period"])
            )

    with open('timepoints_rps.tab','r') as f:
        for r in csv.DictReader(f, delimiter='\t'):
            i += 1
            sys.stdout.write('row: {}\r'.format(i))
            sys.stdout.flush()
            t = r["timestamp"][5:]
            dt = str(r["timeseries"])[2:8]
            execute("""
                INSERT INTO study_hour
                    (time_sample, study_date, study_hour,
                    hour_of_day, date_time)
                VALUES
                    (%s, %s, %s,
                    %s,
                    cast(%s as timestamp with time zone));
            """,
                ('rps', r["timeseries"], r["timepoint_id"],
                int(t[6:8]),
                "20" + dt[0:2] + '-' + t[:5] + ' ' + t[6:] + ':00-10')
            )

    execute("""
        delete from study_periods where time_sample='rps';
        insert into study_periods
            select distinct time_sample, period from study_date
            where time_sample='rps'
            order by 2;

        -- mini RPS study (even months, even hours) for reasonable results in relatively short time

        drop table if exists tdoublemonths;
        create temporary table tdoublemonths
            (month_of_year smallint primary key, days_in_month smallint);
        insert into tdoublemonths values
          (1, 59), (2, 59), (3, 61), (4, 61), (5, 61), (6, 61),
          (7, 62), (8, 62), (9, 61), (10, 61), (11, 61), (12, 61);

        delete from study_date where time_sample='rps_mini';
        insert into study_date
            (period, study_date, month_of_year, date,
            hours_in_sample, time_sample, ts_num_tps, ts_duration_of_tp, ts_scale_to_period)
            select period, study_date, d.month_of_year, date,
                0.0 as hours_in_sample,
                'rps_mini' as time_sample,
                12 as ts_num_tps, 2.0 as ts_duration_of_tp,
                case when ts_scale_to_period < 100 then 2*%(years_per_period)s
                    else (days_in_month-2)*%(years_per_period)s end as ts_scale_to_period
            from study_date d join tdoublemonths m using (month_of_year)
            where time_sample = 'rps' and mod(month_of_year, 2)=0
            order by 1, 3, 5 desc, 4;

        delete from study_hour where time_sample='rps_mini';
        insert into study_hour (study_date, study_hour, hour_of_day, date_time, time_sample)
          select h.study_date, study_hour, hour_of_day, date_time,
            'rps_mini' as time_sample
          from study_hour h join study_date d
            on (d.time_sample='rps_mini' and d.study_date=h.study_date)
          where h.time_sample = 'rps' and mod(hour_of_day, 2)=0
          order by period, month_of_year, hours_in_sample desc, hour_of_day;

        delete from study_periods where time_sample='rps_mini';
        insert into study_periods
            select distinct time_sample, period from study_date
            where time_sample='rps_mini'
            order by 2;


    """, dict(years_per_period=8))


    print("Created rps and rps_mini time samples.")


def save_timesamples_to_postgresql(study_periods, study_dates, study_hours):
    """Write data quickly from pandas dataframes to postgresql database,
    deleting any pre-existing matching records."""

    ts_dict = {'time_sample': tuple(set(study_periods['time_sample']))}

    # study_periods
    execute("delete from study_periods where time_sample in %(time_sample)s;", ts_dict)
    copy_dataframe_to_table(
        study_periods[['time_sample', 'period']],
        'study_periods'
    )

    # study_date
    execute("delete from study_date where time_sample in %(time_sample)s;", ts_dict)
    copy_dataframe_to_table(
        study_dates[[
            'time_sample', 'period', 'study_date',
            'month_of_year', 'date', 'hours_in_sample',
            'ts_num_tps', 'ts_duration_of_tp', 'ts_scale_to_period'
        ]],
        'study_date'
    )

    # study_hour
    execute("delete from study_hour where time_sample in %(time_sample)s;", ts_dict)
    copy_dataframe_to_table(
        study_hours[[
            'time_sample', 'study_date', 'study_hour', 'hour_of_day', 'date_time'
        ]],
        'study_hour'
    )

def mini_timeseries():
    # 2025, 2045; 2007-07-15; hours 0, 4, 8, 12, 16, 20

    # include some < 100% and some 100% RPS
    study_periods = pd.DataFrame({
        'time_sample': 'tiny',
        'period': [2025, 2045]
    })

    tp_duration = 4
    tps_per_ts = int(24/tp_duration)

    study_dates = study_periods.copy()
    study_dates['date'] = datetime.datetime(2007, 7, 15)  # one date per period
    study_dates['month_of_year'] = study_dates['date'].dt.month
    study_dates['ts_duration_of_tp'] = tp_duration
    study_dates['ts_num_tps'] = tps_per_ts
    study_dates['ts_scale_to_period'] = (2045-2025) * 365.25
    study_dates['hours_in_sample'] = study_dates['ts_duration_of_tp'] * study_dates['ts_scale_to_period']
    study_dates['study_date'] = study_dates['period'].mod(100)*100 + study_dates['month_of_year']

    one_day = pd.DataFrame({
        'hour_of_day': np.arange(0, 24, tp_duration),
    })
    study_hours = (
        study_dates.loc[:, ['time_sample', 'study_date', 'date']].assign(dummy=1)
        .merge(one_day.assign(dummy=1))
        .drop('dummy', axis=1)
    )
    dt = study_hours['date'].dt
    study_hours['date_time'] = pd.to_datetime(pd.DataFrame(dict(
        year=dt.year, month=dt.month, day=dt.day, hour=study_hours['hour_of_day']
    )))
    study_hours['study_hour'] = study_hours['study_date'] * 100 + study_hours['hour_of_day']

    save_timesamples_to_postgresql(study_periods, study_dates, study_hours)
    print("Created tiny time samples.")


#########################
# monthly timeseries
def monthly_timeseries():

    print("creating monthly timeseries (takes several minutes).")
    print("you can monitor progress in psql with 'select query from pg_stat_activity;'.")

    #########
    # create time_samples
    time_samples = pd.DataFrame(
        index=pd.MultiIndex.from_product(
            [range(2017, 2056), [2007, 2008], range(1, 13)],
            names = ['period', 'base_year', 'month_of_year']
        )
    ).reset_index()
    time_samples['time_sample'] = time_samples.apply(
        lambda row: 'monthly_{period}_{month_of_year:02d}_{base_year}'.format(**row),
        axis=1
    )

    #########
    # create list of dates and hours in a reference year (based on 2007, a non-leap year)
    year_dates = pd.DataFrame(
        index=pd.date_range(datetime.date(2007, 1, 1), periods=365, freq='D', name='date_time')
    ).reset_index()
    year_dates['month_of_year'] = year_dates['date_time'].dt.month
    year_dates['day_of_month'] = year_dates['date_time'].dt.day

    year_hours = pd.DataFrame(
        index=pd.date_range(
            datetime.datetime(2007, 1, 1, 0, 0, 0), periods=8760, freq='h', name='date_time'
        )
    ).reset_index()
    dt = year_hours['date_time'].dt
    year_hours['month_of_year'] = dt.month
    year_hours['day_of_month'] = dt.day
    year_hours['hour_of_day'] = dt.hour

    #########
    # create timesample dataframes

    study_periods = time_samples[['time_sample', 'period']]

    study_dates = time_samples.merge(year_dates[['month_of_year', 'day_of_month']], on='month_of_year')
    study_dates['study_date'] = (
        (study_dates['period'] * 100 + study_dates['month_of_year']) * 100 + study_dates['day_of_month']
    )
    study_dates['date'] = pd.to_datetime(
        study_dates[['base_year', 'month_of_year', 'day_of_month']]
        .rename(columns={"base_year": "year", "month_of_year": "month", "day_of_month": "day"})
    )
    study_dates['hours_in_sample'] = 1
    study_dates['ts_num_tps'] = 24
    study_dates['ts_duration_of_tp'] = 1
    study_dates['ts_scale_to_period'] = 1

    study_hours = study_dates.merge(
        year_hours[['month_of_year', 'day_of_month', 'hour_of_day']],
        on=['month_of_year', 'day_of_month']
    )
    # might be neater to do the next line as study_hours['date'] + hour_of_day
    study_hours['date_time'] = pd.to_datetime(
        study_hours[['base_year', 'month_of_year', 'day_of_month', 'hour_of_day']]
        .rename(columns={
            "base_year": "year", "month_of_year": "month",
            "day_of_month": "day", "hour_of_day": "hour"
        })
    )
    # study_hours['study_date'] = (
    #     study_hours['period'] * 100 + study_hours['month_of_year']) * 100 + study_hours['day']
    # )
    study_hours['study_hour'] = study_hours['study_date'] * 100 + study_hours['hour_of_day']

    #########
    # put the timesamples in the postgresql database

    save_timesamples_to_postgresql(study_periods, study_dates, study_hours)

    print("Created monthly time samples.")


#########################
# daily timeseries
# one time_sample per day in the years 2017-2055, based on 2007 or 2008 weather,
# excluding leap day and bad days at end of 2008 (see make_short_slice_timeseries)
# time_sample = daily_nnn, where nnn ranges from 000 to 726
def daily_timeseries():

    print("creating daily timeseries (takes several minutes).")
    print("you can monitor progress in psql with 'select query from pg_stat_activity;'.")

    for year in range(2017, 2056):
        make_short_slice_timeseries(
            days_per_sample=1,
            period_years=[year],
            period_lengths=[1.0/365.25],
            time_sample_base='daily_{}'.format(year)
        )

    print("Created daily time samples.")


#########################
# annual slice timeseries
# These are every 10th day, during each year from 2020-2045, weighted to span a full year.
# They allow "production cost models" to enforce full-period constraints while keeping model size small.
def annual_slice_timeseries():

    print("creating annual slice timeseries (takes several minutes).")
    print("you can monitor progress in psql with 'select query from pg_stat_activity;'.")

    #########
    # create time_samples
    time_samples = pd.DataFrame(
        index=pd.MultiIndex.from_product(
            [range(2017, 2056), [2007, 2008], range(10)],
            names = ['period', 'base_year', 'slice']
        )
    ).reset_index()
    time_samples['time_sample'] = time_samples.apply(
        lambda row: 'slice_{period}_{slice}_{base_year}'.format(**row),
        axis=1
    )

    #########
    # create list of dates and hours in a reference year (based on 2007, a non-leap year)
    year_dates = pd.DataFrame(
        index=pd.date_range(datetime.date(2007, 1, 1), periods=365, freq='D', name='date_time')
    ).reset_index()
    dt = year_dates['date_time'].dt
    year_dates['month_of_year'] = dt.month
    year_dates['day_of_month'] = dt.day
    year_dates['slice'] = (dt.dayofyear - 1) % 10

    year_hours = pd.DataFrame(
        index=pd.date_range(
            datetime.datetime(2007, 1, 1, 0, 0, 0), periods=8760, freq='h', name='date_time'
        )
    ).reset_index()
    dt = year_hours['date_time'].dt
    year_hours['month_of_year'] = dt.month
    year_hours['day_of_month'] = dt.day
    year_hours['hour_of_day'] = dt.hour
    year_hours['slice'] = (dt.dayofyear - 1) % 10


    #########
    # create timesample dataframes

    study_periods = time_samples[['time_sample', 'period']]

    study_dates = time_samples.merge(year_dates[['slice', 'month_of_year', 'day_of_month']], on='slice')
    study_dates['study_date'] = (
        (study_dates['period'] * 100 + study_dates['month_of_year']) * 100 + study_dates['day_of_month']
    )
    study_dates['date'] = pd.to_datetime(
        study_dates[['base_year', 'month_of_year', 'day_of_month']]
        .rename(columns={"base_year": "year", "month_of_year": "month", "day_of_month": "day"})
    )
    time_sample_weights = (
        study_dates.groupby('time_sample').size()
        .reset_index().rename(columns={0: 'n_days'})
    )
    study_dates = study_dates.merge(time_sample_weights, on='time_sample')

    study_dates['hours_in_sample'] = 1
    study_dates['ts_num_tps'] = 24
    study_dates['ts_duration_of_tp'] = 1
    study_dates['ts_scale_to_period'] = 365.0 / study_dates['n_days']

    study_hours = study_dates[
        ['period', 'base_year', 'time_sample', 'study_date', 'month_of_year', 'day_of_month']
    ].merge(
        year_hours[['month_of_year', 'day_of_month', 'hour_of_day']],
        on=['month_of_year', 'day_of_month']
    )
    # might be neater to do the next line as study_hours['date'] + hour_of_day
    study_hours['date_time'] = pd.to_datetime(
        study_hours[['base_year', 'month_of_year', 'day_of_month', 'hour_of_day']]
        .rename(columns={
            "base_year": "year", "month_of_year": "month",
            "day_of_month": "day", "hour_of_day": "hour"
        })
    )
    # study_hours['study_date'] = (
    #     study_hours['period'] * 100 + study_hours['month_of_year']) * 100 + study_hours['day']
    # )
    study_hours['study_hour'] = study_hours['study_date'] * 100 + study_hours['hour_of_day']


    #########
    # put the timesamples in the postgresql database

    save_timesamples_to_postgresql(study_periods, study_dates, study_hours)

    print("Created annual slice time samples.")


#########################
# 30-year slice timeseries
# These are every 30th day, during each 5-year period in 2020-2045, weighted to span the full period.
# They allow "production cost models" to incorporate build decisions from all periods and
# enforce full-period constraints while keeping model size small.
# (an alternative would be to use the annual slice timeseries, but that would require custom
# code to pin each build variable to the right level, e.g., convert the 6-period hydrogen construction
# plan into an equivalent single-period plan.)
def long_slice_timeseries():

    print("creating 30-year slice timeseries (takes several minutes).")
    print("you can monitor progress in psql with 'select query from pg_stat_activity;'.")

    n_slices = 30
    years_per_period = 5

    #########
    # create time_samples
    time_samples = pd.DataFrame(
        index=pd.MultiIndex.from_product(
            [[2007, 2008], range(n_slices)],
            names = ['base_year', 'slice']
        )
    ).reset_index()
    time_samples['time_sample'] = time_samples.apply(
        lambda row: 'slice_{slice:02d}_{base_year}'.format(**row),
        axis=1
    )
    time_samples['dummy'] = 1

    periods = pd.DataFrame({"period": range(2020, 2050, 5), "dummy": 1})

    #########
    # create list of dates and hours in a reference year (based on 2007, a non-leap year)
    year_dates = pd.DataFrame(
        index=pd.date_range(datetime.date(2007, 1, 1), periods=365, freq='D', name='date_time')
    ).reset_index()
    dt = year_dates['date_time'].dt
    year_dates['month_of_year'] = dt.month
    year_dates['day_of_month'] = dt.day
    year_dates['slice'] = (dt.dayofyear - 1) % n_slices

    year_hours = pd.DataFrame(
        index=pd.date_range(
            datetime.datetime(2007, 1, 1, 0, 0, 0), periods=8760, freq='h', name='date_time'
        )
    ).reset_index()
    dt = year_hours['date_time'].dt
    year_hours['month_of_year'] = dt.month
    year_hours['day_of_month'] = dt.day
    year_hours['hour_of_day'] = dt.hour
    year_hours['slice'] = (dt.dayofyear - 1) % n_slices

    #########
    # create timesample dataframes

    # do cross-join using dummy columns
    study_periods = time_samples.merge(periods, on='dummy').drop('dummy', axis=1)

    study_dates = study_periods.merge(
        year_dates[['slice', 'month_of_year', 'day_of_month']], on='slice'
    )
    study_dates['study_date'] = (
        (study_dates['period'] * 100 + study_dates['month_of_year']) * 100 + study_dates['day_of_month']
    )
    study_dates['date'] = pd.to_datetime(
        study_dates[['base_year', 'month_of_year', 'day_of_month']]
        .rename(columns={"base_year": "year", "month_of_year": "month", "day_of_month": "day"})
    )
    time_sample_weights = (
        study_dates.groupby(('time_sample', 'period')).size()
        .reset_index().rename(columns={0: 'n_days'})
    )
    study_dates = study_dates.merge(time_sample_weights, on=['time_sample', 'period'])

    study_dates['hours_in_sample'] = 1
    study_dates['ts_num_tps'] = 24
    study_dates['ts_duration_of_tp'] = 1
    study_dates['ts_scale_to_period'] = float(years_per_period * 365) / study_dates['n_days']

    study_hours = study_dates[
        ['period', 'base_year', 'time_sample', 'study_date', 'month_of_year', 'day_of_month']
    ].merge(
        year_hours[['month_of_year', 'day_of_month', 'hour_of_day']],
        on=['month_of_year', 'day_of_month']
    )
    # might be neater to do the next line as study_hours['date'] + hour_of_day
    study_hours['date_time'] = pd.to_datetime(
        study_hours[['base_year', 'month_of_year', 'day_of_month', 'hour_of_day']]
        .rename(columns={
            "base_year": "year", "month_of_year": "month", "day_of_month": "day", "hour_of_day": "hour"
        })
    )
    study_hours['study_hour'] = study_hours['study_date'] * 100 + study_hours['hour_of_day']

    #########
    # put the timesamples in the postgresql database

    save_timesamples_to_postgresql(study_periods, study_dates, study_hours)

    print("Created long slice time samples.")



#########################
# Create "vertical" slices through time for use with progressive hedging algorithm
# Each time_sample includes 5-6 historical days from 2007 and/or 2008 (for fast solution).
# They are spread as well as possible across day-of-year and year
# Covers six 5-year periods from 2020 to 2045. The same historical dates are used for
# all periods.

def year_len(year):
    return (datetime.date(year+1, 1, 1) - datetime.date(year, 1, 1)).days

def pha_timeseries():
    print("Creating pha timeseries.")
    print("You can monitor progress in psql with 'select query from pg_stat_activity;'.")
    periods5 = [2020, 2025, 2030, 2035, 2040, 2045]
    periods5length = [5, 5, 5, 5, 5, 5]
    periods10 = [2020, 2030, 2040, 2045]
    periods10length = [10, 10, 5, 5]
    #make_pha_timeseries(60)
    # should solve very quickly
    make_pha_timeseries(183, periods10, periods10length, 'pha_183_10')

def make_pha_timeseries(day_step, period_years, period_lengths, time_sample_base):
    # day_step is the number of days-of-year to jump between series
    year_sequence = [2007, 2008] # years to rotate between

    year_lengths = [year_len(y) for y in year_sequence]
    n_digits = int(math.ceil(math.log10(day_step*len(year_sequence))))

    date_samples = []
    for day_offset in range(day_step):
        for year_offset in range(len(year_sequence)):
            # start at the right year/day
            year_idx = year_offset
            day_of_year = day_offset
            sample_dates = []
            while day_of_year < year_lengths[year_idx]:
                sample_dates.append(
                    datetime.date(year_sequence[year_idx], 1, 1)
                    + datetime.timedelta(days=day_of_year)
                )
                # increment year/day counters
                year_idx = (year_idx + 1) % len(year_sequence)
                day_of_year += day_step
            date_samples.append(sample_dates)

    # make sure there are no duplicates and that all years are fully covered
    all_dates = sum(date_samples, [])
    assert len(all_dates) == len(set(all_dates))
    assert len(all_dates) == sum(year_lengths)

    make_records_from_sample_dates(
        date_samples, period_years, period_lengths, time_sample_base, n_digits
    )

def make_records_from_sample_dates(
    date_samples, period_years, period_lengths, time_sample_base, n_digits
):
    # date_samples is a list of lists of dates
    # each sublist will be used for one time_sample,
    # repeated across all periods

    time_sample_ids = list(range(len(date_samples)))
    time_samples = [
        time_sample_base + '_' + str(i).zfill(n_digits)
        for i in time_sample_ids
    ]

    # create study_periods records
    study_periods = pd.DataFrame.from_records(
        [
            (time_samples[ti], p, l, ti)
            for ti in time_sample_ids
            for p, l in zip(period_years, period_lengths)
        ],
        columns=['time_sample', 'period', 'period_length', 'time_sample_id']
    )

    # create study_dates records
    study_dates_no_period = pd.DataFrame.from_records(
        [
            (time_samples[i], d, 365.25/len(dates))
            for i, dates in enumerate(date_samples)
            for d in dates
        ], columns=['time_sample', 'date', 'ts_scale_to_year']
    )
    study_dates = study_periods.merge(study_dates_no_period, on='time_sample')
    study_dates['date'] = pd.to_datetime(study_dates['date'])
    dt = study_dates['date'].dt
    study_dates['study_date'] = (study_dates['period']%100)*1000000 + dt.strftime('%y%m%d').astype(int)
    study_dates['month_of_year'] = dt.month
    study_dates['hours_in_sample'] = 1
    study_dates['ts_num_tps'] = 24
    study_dates['ts_duration_of_tp'] = 1
    # number of times each timeseries would need to be repeated to make a full period
    study_dates['ts_scale_to_period'] = study_dates['ts_scale_to_year'] * study_dates['period_length']

    hours_no_date = pd.DataFrame({'hour': pd.timedelta_range(start='0 hour', freq='1H', periods=24)})
    study_hours = (
        study_dates.assign(dummy=1).merge(hours_no_date.assign(dummy=1), on='dummy')
        .drop('dummy', axis=1)
    )
    study_hours['date_time'] = study_hours['date'] + study_hours['hour']
    study_hours['hour_of_day'] = study_hours['date_time'].dt.hour
    study_hours['study_hour'] = study_hours['study_date'] * 100 + study_hours['hour_of_day']

    #########
    # put the timesamples in the postgresql database
    save_timesamples_to_postgresql(study_periods, study_dates, study_hours)

    print("Created time samples {}_*.".format(time_sample_base))

###############################
# Short slice timesamples, for evaluating plans using all dates.
# Each time_sample contains a small number of study days, in sequence but not
# linked together (i.e., each day is a separate study date), for multiple periods.
# The days are equally weighted to cover each period, just like when optimizing
# with Switch, but would not be good choices for optimizing, since each time_sample
# has minimal diversity.

def short_slice_timeseries():
    print("Creating short slice timeseries.")
    print("You can monitor progress in psql with 'select query from pg_stat_activity;'.")

    series_data = {
        5: dict(
            periods = [2020, 2025, 2030, 2035, 2040, 2045],
            lengths = [5, 5, 5, 5, 5, 5],
            base = 'slice_5_1'
        ),
        10: dict(
            periods = [2020, 2030, 2040, 2045],
            lengths = [10, 10, 5, 5],
        ),
        20: dict(
            periods = [2025, 2045],
            lengths = [20, 20],
        ),
    }

    days_per_sample = 1
    for series in [20]:
        s = series_data[series]
        make_short_slice_timeseries(
            days_per_sample=days_per_sample,
            period_years=s['periods'],
            period_lengths=s['lengths'],
            time_sample_base='slice_{}_{}'.format(series, days_per_sample)
        )

def make_short_slice_timeseries(days_per_sample, period_years, period_lengths, time_sample_base):
    # get all dates in 2007-2008 except some with missing or infeasible data
    all_dates = list(
        pd.date_range(
            start=datetime.date(2007, 1, 1),
            end=datetime.date(2008, 12, 31),
            freq='D', name='date'
        )
        .drop(datetime.datetime(2008,  2, 29))  # no solar records in NSRDB
        .drop(datetime.datetime(2008, 12, 26))  # blackout; zero loads are infeasible
        .drop(datetime.datetime(2008, 12, 27))  # blackout; zero loads are infeasible
        .drop(datetime.datetime(2008, 12, 31))  # wind data stops at midnight UTC, 14:00 HST
    )

    n_dates = len(all_dates)
    date_samples = []
    for i in range(0, n_dates, days_per_sample):
        group_end = min(i + days_per_sample, n_dates)
        date_samples.append(all_dates[i:group_end])

    n_digits = 3
    make_records_from_sample_dates(
        date_samples, period_years, period_lengths, time_sample_base, n_digits
    )


# filter to select dates with valid data; used for k-means and nearest-cdf sampling
# we omit these dates (HST):
# 2008-02-29: no solar data available
# 2008-12-27: blackout (can't be modeled)
# 2008-12-26: blackout (can't be modeled)
# 2008-12-31: wind data go up to the end of 2008 in UTC timezone, which is
# 10 hours before the end of 2008 in HST.
sql_date_filter = """
    date_trunc('day', date_time) between '2007-01-01' and '2008-12-31'
    and date_trunc('day', date_time) not in
        ('2008-02-29', '2008-12-26', '2008-12-27', '2008-12-31')
"""

def get_hourly_resource_vectors():
    """
    Return numpy array showing vectors of hourly values for
    'CentralTrackingPV', 'SlopedDistPV', 'OnshoreWind' and load, for each
    historical day for which we have data (most of 2007-2008). These are
    used for k-means clustering.
    """
    # get dataframe with date index, hourly values for mean wind, mean solar, load

    print("Retrieving average hourly production for all technologies...")

    # Note: all values are normalized to a 0-1 scale, so the Euclidian
    # norm used for k-means clustering is somewhat meaningful (may underweight
    # load a bit, since it has less variance and only makes up 1/4 of the data).
    hourly_query = """
        select
            date_time, technology as label,
            sum(max_capacity*cap_factor)/sum(max_capacity) as level
        from project join cap_factor using (project_id)
        where technology in ('CentralTrackingPV', 'SlopedDistPV', 'OnshoreWind')
            and {date_filter}
        group by 1, 2
        union
        select
            -- note: max load in this date range is 1248.9
            date_time, 'SystemLoad' as label, system_load/1250.0 as level
        from system_load
        where {date_filter}
        ;
    """.format(date_filter=sql_date_filter)
    hourly_data = pd.read_sql(hourly_query, con=db_engine)

    # convert hourly data into date-labeled vectors, sorted by label and hour of day.

    # note: dates are stored in the database in HST (and naive dates stored from
    # other timesample functions get converted to HST). But they come back from
    # the database in UTC, so the dates won't split correctly.
    hourly_data['date_time'] = hourly_data['date_time'].dt.tz_convert('Pacific/Honolulu')
    hourly_data['date'] = hourly_data['date_time'].dt.date
    hourly_data['idx'] = hourly_data['label'] + '_' + hourly_data['date_time'].dt.strftime('%H')
    vectors = hourly_data[['idx', 'date', 'level']].pivot(index='date', columns='idx')
    vectors.columns = vectors.columns.droplevel()  # drop "level" heading

    return vectors


def get_daily_resource_vectors():
    """
    Return numpy array showing daily means for
    'CentralTrackingPV', 'OnshoreWind' and load, for each
    historical day for which we have data (most of 2007-2008).
    Returns a matrix with one row per historical day and one column per resource.
    (Used for closest-CDF timeseries.)
    """
    # get dataframe with date index, hourly values for mean wind, mean solar, load

    hourly_vectors = get_hourly_resource_vectors()
    hourly_vectors.columns = [c[:-3] for c in hourly_vectors.columns]
    daily_vectors = (
        hourly_vectors[['CentralTrackingPV', 'OnshoreWind', 'SystemLoad']]
        .T.reset_index().groupby('index').mean().T
    )
    # convert into date-labeled vectors, sorted by label
    return daily_vectors


def k_means_timeseries():
    """
    Create timeseries for various numbers of sample dates with diverse levels of
    load, wind and solar, using k-means clustering.
    """
    f_name = __name__ + '.k_means_timeseries()'

    vectors = get_hourly_resource_vectors()

    period_sets = [
        {
            'period_years': [2020, 2025, 2030, 2035, 2040, 2045],
            'period_length': [5, 5, 5, 5, 5, 5],
            'period_pattern': '5'
        },
        {
            'period_years': [2020, 2022, 2025, 2030, 2035, 2040, 2045],
            'period_length': [2, 3, 5, 5, 5, 5, 5],
            'period_pattern': '235'
        },
        {
            'period_years': [2045],
            'period_length': [1],
            'period_pattern': '2045'
        }
    ]

    # number of days to sample each period
    day_counts = [12, 16]

    # preselected dates (if any).
    # First n-1 are used to recreate corresponding simple clusters if needed.
    # All n are used to create the corresponding extended clusters.
    predefined_dates = {
        12: [
            '2007-08-25', # most dates come from a plain run of k-means without predefined dates
            '2007-05-02',
            '2008-12-04',
            '2007-09-08',
            '2007-12-15',
            '2008-12-08',
            '2008-01-05',
            '2008-03-13',
            '2008-09-10',
            '2007-12-17',
            '2008-10-06',
            '2007-07-09',
            # last (toughest) date found via /s/sampling study
            # '2008-12-13', # with ATB/EIA 2018 assumptions
            '2008-11-22', # with ATB/EIA 2019 assumptions
        ]
    }

    for period_set in period_sets:
        for n_days in day_counts:
            for extended in [False, True]:
                time_sample = 'k_means_{}_{}{}'.format(
                    period_set['period_pattern'],
                    n_days,
                    '+' if extended else ''
                )
                dates=predefined_dates.get(n_days, None)
                if not dates:
                    if extended:
                        print(
                            "No dates stored in {n} for extended k-means "
                            "time_sample {s}. Please run /s/k_means study with "
                            "non-extended version of this sample ({s_short}) and "
                            "then store the initial dates and most difficult "
                            "date in {n}. Skipping {s} for now. "
                            .format(n=f_name, s=time_sample, s_short=time_sample[:-1])
                        )
                        continue
                    else:
                        print(
                            "Choosing new k-means clusters for time_sample {s}. "
                            "Please run /s/k_means study with this time_sample, "
                            "then store the initial dates and most difficult "
                            "date in {n} then re-run this function to create "
                            "the extended version of this time_sample."
                            .format(n=f_name, s=time_sample)
                        )
                if dates and not extended:
                    # drop last date when re-creating non-extended samples
                    dates = dates[:-1]
                make_k_means_timeseries(
                    vectors, n_days,
                    period_set['period_years'], period_set['period_length'],
                    time_sample,
                    dates
                )
                print(
                    "{} time samples {} and {}_2."
                    .format("Re-created" if dates else "Created", time_sample, time_sample)
                )

def k_means_timeseries_daily_avg():
    """
    Create timeseries for various numbers of sample dates with diverse levels of
    load, wind and solar, using k-means clustering.
    """
    f_name = __name__ + '.k_means_timeseries_daily_avg()'

    vectors = get_daily_resource_vectors()

    period_sets = [
        {
            'period_years': [2020, 2025, 2030, 2035, 2040, 2045],
            'period_length': [5, 5, 5, 5, 5, 5],
            'period_pattern': '5'
        },
        {
            'period_years': [2020, 2022, 2025, 2030, 2035, 2040, 2045],
            'period_length': [2, 3, 5, 5, 5, 5, 5],
            'period_pattern': '235'
        },
        {
            'period_years': [2045],
            'period_length': [1],
            'period_pattern': '2045'
        }
    ]

    # number of days to sample each period
    day_counts = [12, 16]

    # preselected dates (if any).
    # First n-1 are used to recreate corresponding simple clusters if needed.
    # All n are used to create the corresponding extended clusters.
    predefined_dates = {
        12: [
            # most dates come from a plain run of k-means without predefined dates
            # then retrieved via
            # psql -c "select date from study_date where time_sample = 'k_means_daily_2045_12';"
            '2008-02-20',
            '2007-02-12',
            '2007-10-27',
            '2008-09-10',
            '2008-04-10',
            '2007-01-12',
            '2008-08-18',
            '2007-05-02',
            '2007-01-19',
            '2008-06-17',
            '2007-09-22',
            '2007-12-15',
            # last (toughest) date found via /s/sampling study;
            # run find_most_expensive_day.py, then
            # psql -c "select date from study_date where time_sample = 'daily_2045_690';"
            '2008-11-22', # slice 690 with 2019 ATB/EIA data
        ]
    }

    for period_set in period_sets:
        for n_days in day_counts:
            for extended in [False, True]:
                time_sample = 'k_means_daily_{}_{}{}'.format(
                    period_set['period_pattern'],
                    n_days,
                    '+' if extended else ''
                )
                dates=predefined_dates.get(n_days, None)
                if not dates:
                    if extended:
                        print(
                            "No dates stored in {n} for extended k-means "
                            "time_sample {s}. Please run /s/sampling study with "
                            "non-extended version of this sample ({s_short}) and "
                            "then store the initial dates and most difficult "
                            "date in {n}. Skipping {s} for now. "
                            .format(n=f_name, s=time_sample, s_short=time_sample[:-1])
                        )
                        continue
                    else:
                        print(
                            "Choosing new k-means clusters for time_sample {s}. "
                            "Please run /s/sampling study with this time_sample, "
                            "then store the initial dates and most difficult "
                            "date in {n} then re-run this function to create "
                            "the extended version of this time_sample."
                            .format(n=f_name, s=time_sample)
                        )
                if dates and not extended:
                    # drop last date when re-creating non-extended samples
                    dates = dates[:-1]
                make_k_means_timeseries(
                    vectors, n_days,
                    period_set['period_years'], period_set['period_length'],
                    time_sample,
                    dates
                )
                print(
                    "{} time samples {} and {}_2."
                    .format("Re-created" if dates else "Created", time_sample, time_sample)
                )


def make_k_means_timeseries(vectors, n_days, period_years, period_length, time_sample, dates=None):
    if dates:
        center_dates = pd.to_datetime(dates)
    else:
        # find good centroids
        kmeans = sklearn.cluster.KMeans(n_clusters=n_days).fit(vectors.values)
        # find closest datapoint to centroids (from https://stackoverflow.com/a/21673353/3830997)
        closest, _ = sklearn.metrics.pairwise_distances_argmin_min(kmeans.cluster_centers_, vectors.values)
        center_dates = vectors.index[closest]

    # re-initialize kmeans using the specified dates, since they are slightly
    # different from the current centroids. This gives a more accurate clustering
    # of other dates around these "central" dates.
    init_vectors = vectors.loc[center_dates, :]
    kmeans = sklearn.cluster.KMeans(
        n_clusters=len(init_vectors),
        init=init_vectors.values, n_init=1,
        max_iter=1
    ).fit(vectors.values)

    # %matplotlib inline
    # selected_data.T.plot(legend=False)
    cluster_counts = np.bincount(kmeans.predict(vectors.values))
    cluster_weight = cluster_counts/cluster_counts.sum()

    selected_dates = pd.DataFrame({'date': center_dates, 'weight': cluster_weight})

    # store timesample for selected dates
    dates = list(selected_dates['date'])
    ts_scale_to_year = list(365 * selected_dates['weight'])
    hour_spacing = 1
    create_time_sample_records(
        dates, ts_scale_to_year, period_years, period_length, hour_spacing, time_sample
    )
    hour_spacing = 2
    time_sample = time_sample + '_2'
    create_time_sample_records(
        dates, ts_scale_to_year, period_years, period_length, hour_spacing, time_sample
    )


def closest_cdf_timeseries():
    daily_vectors = get_daily_resource_vectors()
    # period_years = [2020, 2022, 2025, 2030, 2035, 2040, 2045],
    # period_length = [2, 3, 5, 5, 5, 5, 5],
    # time_sample = 'cdf_235_12'
    period_years = [2045]
    period_length = [1]
    period_tag = '2045'

    # 34s to solve with 5 points
    # 4000s+ (7000s?) to solve with 10 points
    # not solvable with 20 points?

    # workflow:
    # make and store basic time samples with various numbers of points and time limits
    # test these with /s/sampling; find the best day to make them more secure
    # make and store extended time samples with same number of points, maybe no time limit
    configs = [
        dict(
            # this is the only one that actually solves within the time limit (takes 34s)
            n_days=12,
            n_points=5,
            time_limit=60,
            basic_dates=[
                8,
                21,
                53,
                156,
                267,
                312,
                338,
                346,
                374,
                456,
                460,
                557
            ],
            extra_dates=[690]
        ),
        dict(
            n_days=12,
            n_points=6,
            time_limit=1800,
            basic_dates=[
                13,
                64,
                118,
                122,
                214,
                245,
                339,
                419,
                443,
                513,
                627,
                634
            ],
            extra_dates=[336]
        ),
        dict(
            n_days=12,
            n_points=7,
            time_limit=60,
            basic_dates=[
                8,
                80,
                88,
                141,
                177,
                383,
                439,
                485,
                496,
                612,
                654,
                717,
            ],
            extra_dates=[336]
        ),
        dict(
            n_days=12,
            n_points=7,
            time_limit=1800,
            basic_dates=[
                63,
                80,
                89,
                177,
                257,
                305,
                420,
                456,
                467,
                496,
                612,
                717,
            ],
            extra_dates=[690]
        ),
        dict(
            n_days=12,
            n_points=10,
            time_limit=60,
            basic_dates=[
                5,
                74,
                148,
                200,
                214,
                417,
                485,
                529,
                608,
                643,
            ],
            extra_dates=[690]
        ),
    ]

    # Add dates selected via k-means clustering (with hourly and daily vectors)
    k_means_dates = {
        'k_means_hourly_cdf': [
            '2007-08-25', # most dates come from a plain run of k-means without predefined dates
            '2007-05-02',
            '2008-12-04',
            '2007-09-08',
            '2007-12-15',
            '2008-12-08',
            '2008-01-05',
            '2008-03-13',
            '2008-09-10',
            '2007-12-17',
            '2008-10-06',
            '2007-07-09',
            # last (toughest) date found via /s/sampling study
            '2007-12-03', # slice 336 with ATB/EIA 2019 assumptions
        ],
        'k_means_daily_cdf': [
            # most dates come from a plain run of k-means without predefined dates
            # then retrieved via
            # psql -c "select date from study_date where time_sample = 'k_means_daily_2045_12';"
            '2008-02-20',
            '2007-02-12',
            '2007-10-27',
            '2008-09-10',
            '2008-04-10',
            '2007-01-12',
            '2008-08-18',
            '2007-05-02',
            '2007-01-19',
            '2008-06-17',
            '2007-09-22',
            '2007-12-15',
            # last (toughest) date found via /s/sampling study;
            # run find_most_expensive_day.py, then
            # psql -c "select date from study_date where time_sample = 'daily_2045_690';"
            '2008-11-22', # slice 690 with 2019 ATB/EIA data
        ],
    }
    def date_to_idx(date):
        return daily_vectors.index.get_loc(pd.to_datetime(date))
    for tag, dates in k_means_dates.items():
        configs.append(dict(
            tag=tag,
            n_days=12,
            n_points=20,
            time_limit=1200,
            basic_dates=[date_to_idx(d) for d in dates[:12]],
            extra_dates=[date_to_idx(d) for d in dates[12:]]
        ))


    for c in configs:
        if not c['basic_dates']:
            continue
        # c = configs[-2]
        if 'tag' not in c:
            c['tag'] = 'cdf'
        time_sample = (
            '{tag}_{n_points}_{time_limit}_{period_tag}_{n_days}'
            .format(period_tag=period_tag, **c)
        )
        print("Finding {} for {}.".format(
            'basic weights' if c['basic_dates'] else 'basic dates and weights',
            c
        ))
        # n_days = c['n_days']; n_points = c['n_points'];
        # time_limit = c['time_limit']; date_ids = c['basic_dates']
        results = make_closest_cdf_timeseries(
            daily_vectors, c['n_days'], period_years, period_length, time_sample,
            c['n_points'], c['time_limit'], c['basic_dates']
        )
        print("Dates and weights for {} time_sample:".format(time_sample))
        for r in results:
            print(r)
        if not c['basic_dates']:
            print("These should be saved in {}.closest_cdf_timeseries() for reuse later.".format(__name__))
            print("You should also run a model with these dates in /s/sampling to find the hardest to serve day and add that to this list.")
        # create a time sample for the extended date set if possible
        if c['extra_dates']:
            time_sample += '+'
            print("Creating {} time_sample:".format(time_sample))
            results = make_closest_cdf_timeseries(
                daily_vectors, c['n_days']+1, period_years, period_length, time_sample,
                c['n_points'], c['time_limit'], c['basic_dates'] + c['extra_dates']
            )
            print("Dates and weights for {} time_sample:".format(time_sample))
            for r in results:
                print(r)

    # note: after testing with /s/sampling, the 5-point solution that solved to
    # the end produced better results than the 7-point solution stopped after 60s,
    # which produced better result than the 10-point solution stopped after 60s.
    # But all of these may be worse than the k-means extended solution.

    # NOTE: these timed out after 1 hour while doing the construction plan
    # (within 0.01% of optimality):
    # cdf_6_1800_2045_12
    # cdf_7_60_2045_12+
    # k_means_daily_cdf_20_1200_2045_12+

    # weirdly, the extra dates always get added in with a weight of zero and
    # have no effect on the weights given to other dates.

    # TODO:
    # . try doing k-means with daily vectors instead of hourly
    # . try using k-means (daily or hourly) to select base vectors, then
    #       use MILP to adjust weights to minimize error in CDF.


def make_closest_cdf_timeseries(
        daily_vectors, n_days, period_years, period_length, time_sample,
        n_points, time_limit, date_ids=None
):
    import operator, pyomo.environ as pyo

    """ Use a mixed-integer linear program to choose dates that minimize the
    difference between the empirical CDF of selected, weighted dates and
    empirical CDF of daily PV, wind and loads."""

    # number of CDF measurement points in each dimension
    # n_points = 6

    levels = daily_vectors.values

    # receive matrix of days * (wind, solar, load) levels (daily_vectors)

    # create matrix of locations of measurement points * (x, y, z) locations
    ranges = np.linspace(levels.min(axis=0), levels.max(axis=0), n_points)
    n_dims = ranges.shape[1]
    points = np.array([
        dim.flatten()
        for dim in np.meshgrid(*(ranges.T))
    ]).T

    # for each octant (l1 <= x, l1 >= x / l2 <= y, l2 >= y / l3 <=z, l3 >= z):
    #    create matrix with one row for each point, one col for each day;
    #        value is bool test of whether level is in that octant around this point
    #    remove points with the duplicate row value (cdf contributors)

    # create list of all possible comparison operations
    operations = [[]]
    for d in range(n_dims):
        old_operations = operations
        operations = []
        for oper in [operator.ge, operator.le]:
            for o in old_operations:
                operations.append([oper] + o)

    test_list = []
    for comparisons in operations:
        in_quadrant = True
        for d, oper in enumerate(comparisons):
            in_quadrant = np.logical_and(oper(points[:, [d]], levels[:, [d]].T), in_quadrant)
        test_list.append(in_quadrant)
    test_points = np.unique(np.concatenate(test_list), axis=0)
    test_points.shape
    # Note: finding unique points can be slow (slower than sorting and probably
    # slower than pd.unique), but it's fast enough with any solvable model.
    # It also only reduces the set by 50-75%, but that's probably worthwhile.

    m = pyo.ConcreteModel()
    m.HIST_DAYS = pyo.Set(initialize=range(test_points.shape[1]))
    # which historical days will be selected and how much probability weight will they get?
    m.Selected = pyo.Var(m.HIST_DAYS, within=pyo.Binary)
    m.Weight = pyo.Var(m.HIST_DAYS, within=pyo.PercentFraction)
    m.only_weight_selected = pyo.Constraint(
        m.HIST_DAYS,
        rule=lambda m, d: m.Weight[d] <= m.Selected[d]
    )
    m.unit_probability = pyo.Constraint(
        rule=lambda m: sum(m.Weight[d] for d in m.HIST_DAYS) == 1.0
    )
    m.limit_selection_count = pyo.Constraint(
        rule=lambda m: sum(m.Selected[d] for d in m.HIST_DAYS) == n_days
    )
    # use preselected dates if specified
    if date_ids:
        m.fix_dates = pyo.Constraint(
            date_ids,
            rule=lambda m, d: m.Selected[d] == 1
        )
    m.TEST_POINTS = pyo.Set(initialize=range(test_points.shape[0]))
    m.MaxError = pyo.Var(within=pyo.NonNegativeReals)
    def rule(m, t):
        actual = test_points[t].mean()
        test = sum(m.Weight[d] for d, incl in enumerate(test_points[t]) if incl)
        return test - actual
    m.Error = pyo.Expression(m.TEST_POINTS, rule=rule)
    m.calculate_MaxError_up = pyo.Constraint(
        m.TEST_POINTS, rule=lambda m, t: m.Error[t] <= m.MaxError
    )
    m.calculate_MaxError_down = pyo.Constraint(
        m.TEST_POINTS, rule=lambda m, t: -m.MaxError <= m.Error[t]
    )
    m.min_MaxError = pyo.Objective(rule=lambda m: m.MaxError, sense=pyo.minimize)
    opt = pyo.SolverFactory('cplex')
    opt.solve(m, tee=True, options_string='time={}'.format(time_limit))

    # extra code to work with previous results
    dw = [(k, pyo.value(v)) for k, v in m.Weight.items() if pyo.value(m.Selected[k]) > 0]
    dw

    dates, ts_scale_to_year = np.array([
        (daily_vectors.index[d], w * 365.25)
        for d, w in dw
    ]).T
    # store timesample for selected dates
    hour_spacing = 1
    create_time_sample_records(
        dates, ts_scale_to_year, period_years, period_length, hour_spacing, time_sample
    )
    hour_spacing = 2
    time_sample = time_sample + '_2'
    create_time_sample_records(
        dates, ts_scale_to_year, period_years, period_length, hour_spacing, time_sample
    )

    return dw


def create_time_sample_records(
    dates, ts_scale_to_year, period_years, period_length, hour_spacing, time_sample
):
    # create study_periods records
    study_periods = pd.DataFrame({
        'time_sample': time_sample,
        'period': period_years,
        'period_length': period_length
    })

    # create study_dates records
    study_dates_no_period = pd.DataFrame({
        'time_sample': time_sample,
        'date': dates,
        'ts_scale_to_year': ts_scale_to_year
    })

    study_dates = study_periods.merge(study_dates_no_period, on='time_sample')
    study_dates['date'] = pd.to_datetime(study_dates['date'])
    dt = study_dates['date'].dt
    study_dates['study_date'] = (study_dates['period']%100)*1000000 + dt.strftime('%y%m%d').astype(int)
    study_dates['month_of_year'] = dt.month
    study_dates['hours_in_sample'] = hour_spacing
    num_tps = int(24.0/hour_spacing)
    assert(num_tps==24.0/hour_spacing)  # must have whole number of tps
    study_dates['ts_num_tps'] = num_tps
    study_dates['ts_duration_of_tp'] = hour_spacing
    # number of times each timeseries would need to be repeated to make a full period
    study_dates['ts_scale_to_period'] = study_dates['ts_scale_to_year'] * study_dates['period_length']

    hours_no_date = pd.DataFrame({
        'hour': pd.timedelta_range(
            start='0 hour', freq='{}H'.format(hour_spacing), periods=num_tps
        ),
        'time_sample': time_sample
    })
    study_hours = study_dates.merge(hours_no_date, on='time_sample')
    study_hours['date_time'] = study_hours['date'] + study_hours['hour']
    study_hours['hour_of_day'] = study_hours['date_time'].dt.hour
    study_hours['study_hour'] = study_hours['study_date'] * 100 + study_hours['hour_of_day']

    #########
    # put the timesamples in the postgresql database

    save_timesamples_to_postgresql(study_periods, study_dates, study_hours)

    print("Created time sample {}.".format(time_sample))


#########################
# ev adoption
def ev_adoption():
    # identify pairs of (ev_scen_id, HECO scenario name):
    ev_adoption_scenarios=(
        'Business as Usual', # straight line to 4.3% by 2045
        'No Burning Desire', # 2013 IRP, 17.5% by 2045
        'Stuck in the Middle', # 2013 IRP, a.k.a. 'Moved by Passion', 35.2% by 2045
        'Blazing a Bold Frontier', # 2013 IRP, 70.1% by 2045
        'PSIP 2016-12', # about 55% by 2045
        'Full Adoption', # 100% by 2045
        'Half Adoption', # 50% by 2045
        'Flat 2016', # 0.5% all the way through
        'EoT 2018', # adoption model up to 55% of LDVs by 2045, from HECO Electrification of Transport study
    )
    # get the EV adoption curves from an Excel workbook
    # uses logistic curves fitted to HECO IRP 2013 Appendix E-10, p. E-113,
    # as well as VMT data from DBEDT Economic Databook
    # and vehicle registration rates from DBEDT monthly energy spreadsheet
    ev_adoption_curves = get_table_from_xlsx(
        data_dir("EV Adoption", "EV projections with buses.xlsx"),
        named_range='ev_data'
    )

    # create the ev_adoption table
    execute("""
        DROP TABLE IF EXISTS ev_adoption;
        CREATE TABLE ev_adoption (
            load_zone varchar(40),
            year int,
            ev_scenario varchar(40),
            ev_share float,
            ice_miles_per_gallon float,
            ev_miles_per_kwh float,
            ev_extra_cost_per_vehicle_year float,
            n_all_vehicles float,
            vmt_per_vehicle float
        );
    """)

    # insert data into the ev_adoption table
    n_rows = len(ev_adoption_curves['Year'])
    for ev_scenario in ev_adoption_scenarios:
        executemany(
            "INSERT INTO ev_adoption VALUES ({})".format(','.join(["%s"]*9)),
            list(zip(
                ['Oahu']*n_rows,
                ev_adoption_curves['Year'],
                [ev_scenario]*n_rows,
                ev_adoption_curves[ev_scenario], # % adoption
                ev_adoption_curves["ICE miles per gallon"],
                ev_adoption_curves["EV miles per kWh"],
                ev_adoption_curves["EV extra cost per vehicle per year"],
                ev_adoption_curves["number of vehicles"],
                ev_adoption_curves["VMT per vehicle"],
            ))
        )

    # add Ulupono data series
    # NOTE: for this series, we are only interested in the EVs, so we model them as if they
    # were the whole fleet. That way, when we report total costs, they don't include any ICE costs.
    uev_scenario = 'Coffman - Ulupono'

    uev = data_frame_from_xlsx(
        data_dir('Ulupono', 'Project EV Electricity Demand_Coffman Reference.xlsx'),
        'ev_data'
    ).T.set_index(0).T
    uev['load_zone'] = 'Oahu'
    uev=uev.rename(columns={"Year": "year"}).set_index(['load_zone', 'year'])

    uev_final = pd.DataFrame(dict(
    ev_scenario=uev_scenario,
    ev_share=1.0,
    ice_miles_per_gallon=30,    # arbitary value, not used
    ev_miles_per_kwh=
        (uev["# of EV's on Road"].values * uev["VMT per Vehicle"].values).sum(axis=1)
        / uev["Electricity (GWh)"].sum(axis=1) / 1e6,
    ev_extra_cost_per_vehicle_year=0.0,
    n_all_vehicles=uev["# of EV's on Road"].sum(axis=1),
    vmt_per_vehicle=
        (uev["# of EV's on Road"].values * uev["VMT per Vehicle"].values).sum(axis=1)
        / uev["# of EV's on Road"].sum(axis=1)
    ))
    # verify it matches the spreadsheet:
    print (
        "The following values should match the energy consumption in "
        + data_dir('Ulupono', 'Project EV Electricity Demand_Coffman Reference.xlsx')
    )
    print (
        uev_final['n_all_vehicles']
        * uev_final['ev_share']
        * uev_final['vmt_per_vehicle']
        / uev_final['ev_miles_per_kwh']
        / 1e6
    )

    # drop existing records
    execute("""
        DELETE FROM ev_adoption WHERE ev_scenario=%s;
    """, (uev_scenario,))
    uev_final.to_sql('ev_adoption', db_engine, if_exists='append')

    # set n_all_vecicles = total number of EVs each year (cars and trucks)
    # set efficiency and vmt per vehicle based on this
    # set ice efficiency to some arbitrary number (e.g., 30 mpg)

    print("Created ev_adoption table.")

    # create the ev_hourly_charge_profile table (simple business-as-usual charging profile,
    # given as hourly weights)
    # see /Users/matthias/Dropbox/Research/shared/Paritosh/M.S Thesis Paritosh
    # /Data Used In Thesis/calculate BAU charging.ipynb
    execute("""
        DROP TABLE IF EXISTS ev_hourly_charge_profile;
        CREATE TABLE ev_hourly_charge_profile (
            hour_of_day smallint,
            charge_weight float
        );
    """)
    with open(data_dir('EV Adoption', 'ev_hourly_charge_profile.tsv')) as f:
        profile = [r.split("\t") for r in f.read().splitlines()][1:] # skip headers

    executemany(
        "INSERT INTO ev_hourly_charge_profile (hour_of_day, charge_weight) VALUES (%s, %s);",
        profile
    )
    print("Created ev_hourly_charge_profile table.")

def ev_adoption_advanced():
    """
    data stored in database:
    ev_charging_bids:
    bids for 1, 2, 3 or 4 hours-per-timestep, showing vectors of total electricity consumption
    each timestep for 100% electric fleet, divided among vehicle classes.
    ev_fleet_data:
    total gasoline and diesel each year for 100% fossil fleet
    capital cost recovery each year for incremental capital cost for a 100% electric fleet
    then main model can choose fractionally between these and set costs accordingly
    """

    ev_workbook = data_dir("EV Adoption", "EV projections with buses.xlsx")
    ev_bid_step_sizes = [1, 2, 3, 4]

    # these are all calculated as [kWh/gal (thermal)] / [EV energy efficiency ratio]
    # see "EV projections with buses.xlsx" for these coefficients
    # They are based on Btu/gal and Btu/kWh from https://www.eia.gov/Energyexplained/?page=about_energy_units
    # and efficiency for trucks and buses in carb_battery_2018 Fig. 1,
    # https://www.arb.ca.gov/msprog/actruck/docs/180124hdbevefficiency.pdf
    # and efficiency for 2017 Nissan Leaf vs. Nissan Versa
    # and 2017 Ford Focus electric vs. titanium from fueleconomy.gov
    car_kwh_per_gal = 35.31 / 3.37
    truck_kwh_per_gal = 40.28 / 4.39
    bus_kwh_per_gal = 40.28 / 4.81

    # get fleet statistics from EV workbook
    vehicle_data = data_frame_from_xlsx(ev_workbook, 'vehicle_data') \
        .T.set_index(0).T.set_index('vehicle type')
    # convert most columns to numeric values (possibly nans)
    # pd.DataFrame.convert_objects() does this nicely, but is deprecated.
    for c in vehicle_data.columns:
        if c not in {'load_zone', 'ICE fuel'}:
            vehicle_data[c] = pd.to_numeric(vehicle_data[c], errors='coerce')

    if len(vehicle_data['load_zone'].unique()) != 1:
        raise NotImplementedError(
            'ev_adoption_advanced() needs to be updated to work with multiple load zones.'
        )

    ##############
    # private vehicle charging profiles

    pgv = 'Passenger gasoline vehicles'  # shorthand lookup key for gas vehicles
    private_ev_work_charging_share = get_named_cell_from_xlsx(ev_workbook, 'private_ev_work_charging_share')
    days_in_reference_year = get_named_cell_from_xlsx(ev_workbook, 'days_in_reference_year')

    # we assume people wait to recharge until the battery needs a 24 kWh recharge
    # (roughly half of a low-end Model 3 battery, or 100% of an older Leaf battery),
    # which takes 4 hours on a 6 kW charger, or until it will need to charge for
    # full plug-in time, whichever is shorter.
    # (could assume plugging in every day in intense demand response cases, but
    # hard to know how people will balance convenience vs. savings)
    target_charge_size = 24
    ev_charger_rating = vehicle_data.loc[pgv, 'charger rating (kW)']

    # travel records from 2017 National Household Travel Survey

    ######################
    # household-level records
    hh = pd.read_csv(data_dir('EV Adoption', 'NHTS 2017', 'hhpub.csv'))
    # Switch column names to lowercase for convenience
    hh.columns = map(str.lower, hh.columns)
    # only use Oahu households
    hh = hh.query("hhstate=='HI' and msacat==3")
    # hh.shape
    # show_index(hh.columns)
    # hh.head()

    ######################
    # vehicle-level data
    # mileage by vehicle class and year from Appendix D of us_epa_light-duty_2018,
    # https://www.epa.gov/fuel-economy-trends/report-tables-and-appendices-co2-and-fuel-economy-trends
    # alternatively, efficiency by vehicle class are shown in afdc_alternative_2015,
    # https://www.afdc.energy.gov/data/10310
    mpg = pd.read_excel(data_dir('EV Adoption', 'EPA Fuel Economy 2017', '420r18001-app-d.xlsx'), header=None)
    mpg.columns = (mpg.loc[0, :].astype(str) + ' ' + mpg.loc[1, :].astype(str)).str.replace('\n', ' ')
    mpg = mpg.loc[2:, :]
    # show_index(mpg.columns)
    mpg = mpg.loc[
        mpg['Vehicle Type nan'] != 'All',
        ['Vehicle Type nan', 'Model Year nan', 'nan Adj Comb']
    ]
    mpg.columns = ['epa_type', 'vehyear', 'mpg']
    mpg['mpg'] = mpg['mpg'].astype(float)
    # mpg.groupby('type').size()
    # add dummy records for motorcycles (mpg from afdc_alternative_2015)
    motorcycle_mpg = mpg[mpg['epa_type']=='Car'].copy()
    motorcycle_mpg[['epa_type', 'mpg']] = ['Motorcycle', 43.54]
    mpg = mpg.append(motorcycle_mpg).reset_index(drop=True)

    # cross-reference NHTS vehicle type to EPA vehicle type
    # (based on https://nhts.ornl.gov/assets/codebook.pdf)
    nhts_epa_map = {
        1: 'Car',
        2: 'Van',
        3: 'Truck SUV',
        4: 'Pickup',
        5: 'Pickup',  # other truck, model as equivalent to pickup truck
        7: 'Motorcycle',
    }

    # vehicle-level data
    veh = pd.read_csv(data_dir('EV Adoption', 'NHTS 2017', 'vehpub.csv'))
    veh.columns = map(str.lower, veh.columns)
    # only use Oahu vehicles
    veh = veh.merge(hh[['houseid']], on='houseid', how='inner')
    # show_index(veh)
    # veh.groupby(['vehtype']).size()
    # treat unknown/not-specified vehicles as 2013 cars
    veh.loc[veh['vehtype'] < 0, 'vehtype'] = 1
    veh.loc[veh['vehyear'] < 0, 'vehyear'] = 2013

    veh['epa_type'] = veh['vehtype'].map(nhts_epa_map)
    veh = veh.merge(mpg, on=['epa_type', 'vehyear'], how='left')
    veh['vehicle'] = veh['houseid'] * 100 + veh['vehid']

    check = veh.loc[
        veh['mpg'].isnull(),
        ['vehtype', 'vehyear', 'epa_type', 'mpg']
    ]
    if check.size > 0:
        print(check)
        ValueError(
            "No mpg data assigned for some vehicles; please correct these before continuing"
        )


    ######################
    # data for all trips by individual household members (will be converted to
    # dwell-time records for each vehicle at home or work)
    trip1 = pd.read_csv(data_dir('EV Adoption', 'NHTS 2017', 'trippub.csv'))
    trip = trip1.copy()
    trip.columns = map(str.lower, trip.columns)
    # show_index(trip.columns)
    # only use Oahu trips
    trip = trip.merge(hh[['houseid']], on='houseid', how='inner')
    # only use car trips, and only use the records for the driver (not passengers)
    trip = trip.query("vehid >= 0 and whodrove==personid").copy()
    # assign unique vehicle ID
    trip['vehicle'] = 100 * trip['houseid'] + trip['vehid']
    # only keep useful columns (makes viewing easier)
    trip = trip[['houseid', 'vehicle', 'strttime', 'endtime', 'whyto', 'loop_trip', 'vmt_mile']]
    # sort the table and get a new, sequential index
    trip = trip.sort_values(['vehicle', 'strttime'], axis=0).reset_index(drop=True)
    trip['trip_id'] = trip.index

    # make sure the same vehicle is never reported on two trips at the same time
    check = trip[['trip_id', 'vehicle', 'strttime', 'endtime']]
    check = check.merge(check, on='vehicle').query(
        "trip_id_x != trip_id_y and endtime_x > strttime_y and strttime_x < endtime_y"
    )
    if check.size > 0:
        print(check)
        raise ValueError('Overlapping trips found in Oahu trip list; please correct before continuing.')

    # chain endtime to next strtime (possibly wrapping back to start of day)
    # note: we assume the first trip the next day will be at the same time as the
    # first trip today.
    trip['nextstrt'] = trip.groupby('vehicle')['strttime'].transform(lambda x: np.roll(x.values, -1))
    # trip[['vehicle', 'strttime', 'endtime', 'nextstrt', 'whyto', 'dweltime']]

    # find/fix loop trips (loop_trip==1)
    # this one seems to be an error, so we adjust it
    trip.loc[  # convert to commute home instead of work-work loop
        (trip["vehicle"]==4064406401) & (trip['endtime']==1855),
        ['whyto', 'loop_trip']
    ] = [1, 2]
    check = trip.query('loop_trip==1 and not (whyto==1 and nextstrt==strttime)')
    check = (
        trip[['vehicle', 'strttime', 'endtime', 'nextstrt', 'whyto', 'loop_trip']]
        .merge(check[['vehicle']], how='inner')
    )
    if check.size > 0:
        show_all(check)
        ValueError(
            "Loop trips found in trip list (other than single home-home trip). "
            "Please correct these before continuing"
        )

    # vehicle location after each trip (based on https://nhts.ornl.gov/assets/codebook.pdf)
    loc_map = {
        1: 'home',
        2: 'home',  # working from home
        3: 'work',
    }
    trip['location'] = trip['whyto'].map(loc_map)

    # Check for vehicles that have no charging location before we drop them.
    # There are a few, but we can safely ignore them.
    # check = trip[['vehicle', 'strttime', 'endtime', 'nextstrt', 'whyto', 'location', 'num_windows']]
    # check['num_windows'] = check.groupby('vehicle')['location'].transform(lambda x: x.dropna().size)
    # check = check[check['num_windows']==0]
    # check[['vehicle', 'strttime', 'endtime', 'nextstrt', 'whyto', 'location', 'num_windows']]

    # convert NHTS timestamps into floating point hours in the day
    make_time = lambda t: t // 100 + (t % 100) / 60
    trip['window_start'] = trip['endtime'].apply(make_time)
    trip['window_end'] = trip['nextstrt'].apply(make_time)
    trip['window_len'] = (trip['window_end'] - trip['window_start']) % 24
    # trip.head()

    # calculate daily travel statistics
    day_miles = trip.groupby('vehicle')['vmt_mile'].sum().reset_index(name='day_miles')
    if 'day_miles' in veh.columns: # may happen when running interactively
        del veh['day_miles']
    veh = veh.merge(day_miles, on='vehicle', how='inner')
    veh['day_gal'] = veh['day_miles'] / veh['mpg']
    veh['day_kwh'] = veh['day_gal'] * car_kwh_per_gal

    # calculate charging time if they use each individual window (only)
    # days_per_charge is how long they would wait if using this particular charging window
    trip = trip.merge(veh[['vehicle', 'day_gal', 'day_kwh']])
    trip['charger_rating'] = ev_charger_rating

    trip['days_per_charge'] = pd.concat([
        # wait till a large charge is needed
        (target_charge_size/trip['day_kwh']).round(),
        # wait till most of the charging window will be needed
        (trip['window_len'] * trip['charger_rating'] / trip['day_kwh']).astype(int).astype(float) # truncate
    ], axis=1).min(axis=1).clip(lower=1)
    trip['charge_duration'] = trip['day_kwh'] * trip['days_per_charge'] / trip['charger_rating']
    # trip.head()
    # trip[['vehicle', 'location', 'window_len', 'day_kwh', 'days_per_charge']]

    # find the longest windows at work and home
    long_home_idx = trip.loc[trip['location']=='home'].groupby('vehicle')['window_len'].idxmax()
    long_work_idx = trip.loc[trip['location']=='work'].groupby('vehicle')['window_len'].idxmax()

    # create records for all home and work charging opportunities (may be multiple per vehicle)
    windows = pd.concat([trip.loc[long_home_idx, :], trip.loc[long_work_idx, :]])
    # windows = windows[[
    #     'houseid', 'vehicle', 'location',
    #     'window_start', 'window_end', 'window_len',
    #     'day_gal', 'day_kwh', 'days_per_charge', 'charge_duration'
    # ]]

    # drop charging windows that are too short
    # (most of these are at work and vehicles have another chance at home)
    windows = windows.query('window_len >= charge_duration')

    # Find vehicles that never get a chance to charge. Turns out these are
    # generally bad data -- only show one trip in the day and no return.
    # So we ignore them. (Actually these disappear completely when we set
    # vehicles to do less than target_kwh_charge if the available window is
    # short.)
    # show_all(trip[~trip['vehicle'].isin(windows['vehicle'])])

    # filter vehicle table to only include vehicles with valid data (and charging windows)
    veh = veh[veh['vehicle'].isin(windows['vehicle'])].copy() # avoid assign with copy warning
    # get scaled weights and multipliers for each vehicle (derived from household weights)
    veh['weight'] = veh['wthhfin']/veh['wthhfin'].sum()
    veh['n_vehicles'] = veh['weight'] * vehicle_data.loc[pgv, 'number of vehicles']

    # find fraction of vehicles with each type of charging opportunity
    # (1 for home, 2 for work, 3 for both)
    home_vehicles = windows.loc[windows['location']=='home', 'vehicle']
    work_vehicles = windows.loc[windows['location']=='work', 'vehicle']
    veh['charge_loc'] = 0
    veh.loc[veh['vehicle'].isin(home_vehicles), 'charge_loc'] += 1
    veh.loc[veh['vehicle'].isin(work_vehicles), 'charge_loc'] += 2
    # Do we need to do 2-window charging at work? Still seems like a lot of
    # vehicles can only charge at home. But maybe they don't do many miles?

    # choose what fraction of each slice of the vehicle fleet will charge at work
    slice_shares = veh.groupby('charge_loc')['weight'].sum()
    # fraction of dual-option vehicles that must charge at work
    mixed_work_share = (
        (private_ev_work_charging_share - slice_shares[2])  # additional at-work charging needed
        / slice_shares[3]               # mixed-mode size
    )
    veh['work_share'] = 0.0
    veh.loc[veh['charge_loc']==1, 'work_share'] = 0  # can only charge at home
    veh.loc[veh['charge_loc']==2, 'work_share'] = 1  # can only charge at work
    veh.loc[veh['charge_loc']==3, 'work_share'] = mixed_work_share
    print("{:.1%} of vehicles that could charge at work or home will charge at work.".format(mixed_work_share))
    print("{:.1%} of all private vehicles will charge at work.".format(
        (veh['work_share'] * veh['weight']).sum()
    ))
    print("{:.1%} of private vehicles are at work long enough to charge in one session.".format(
        1 - veh.loc[veh['charge_loc']==1, 'weight'].sum()
    ))
    if mixed_work_share < 0 or mixed_work_share > 1:
        raise ValueError(
            "Unable to assign correct shares of vehicles for home or "
            "work charging; please correct this before continuing."
        )
    # assign vehicle counts back to the charging window table
    n_vehicles = (
        windows[['trip_id', 'vehicle', 'location']]
        .merge(veh[['vehicle', 'work_share', 'n_vehicles']], on='vehicle')
    )
    n_vehicles['n_vehicles'] = n_vehicles.apply(
        lambda r:
            r['n_vehicles']
            * (r['work_share'] if r['location'] == "work" else 1 - r['work_share']),
        axis=1
    )
    windows = windows.merge(n_vehicles[['trip_id', 'n_vehicles']], on='trip_id')

    target_gals_per_year = vehicle_data.loc[pgv, 'gals fuel per year (2020)']
    nhts_gals_per_year = (veh['day_gal'] * veh['weight']).sum() * days_in_reference_year
    annual_fuel_adjustment = target_gals_per_year / nhts_gals_per_year
    windows['energy_adjustment'] = annual_fuel_adjustment
    print("Adjusting fuel consumption of sample vehicles by {:.4f} to match aggregate data." \
        .format(annual_fuel_adjustment))

    # total_gas = days_in_reference_year * (windows['day_gal'] * windows['n_vehicles'] *
    # windows['energy_adjustment']).sum()

    ###########################
    # now create a table with charging/fuel records for all vehicle types

    # private vehicles
    cols = [
        'vehicle_type', 'veh_id', 'n_vehicles',
        'day_gal', 'day_gal_2045', 'day_kwh',
        'window_start', 'window_end', 'window_len', 'window_start_timestep',
        'days_per_charge', 'charger_rating', 'charge_duration',
        'energy_adjustment',
    ]
    w = windows.reindex(columns=cols).reset_index(drop=True).copy()
    w['vehicle_type'] = pgv
    w['day_gal_2045'] = (
        w['day_gal']
        * vehicle_data.loc[pgv, 'gals fuel per year (2045)']
        / vehicle_data.loc[pgv, 'gals fuel per year (2020)']
    )

    # other vehicles (treated as large, homogenous blocks with some variation in start/end times)
    # create records with a range of start/stop times
    v = vehicle_data.loc[vehicle_data.index != pgv, :].reset_index()
    v['dummy'] = 1  # used for cross-join merging
    spread = pd.DataFrame({'frac': [0.0, 0.25, 0.5, 0.75, 1.0], 'dummy': 1})
    v = v.merge(spread, on='dummy')
    v['charge start'] = v['early charge start'] * (1 - v['frac']) + v['late charge start'] * v['frac']
    v['charge end'] = v['early charge end'] * (1 - v['frac']) + v['late charge end'] * v['frac']
    # consolidate records with identical start/stop times
    v = v.groupby(['vehicle type', 'charge start', 'charge end']).first().reset_index()
    v['number of vehicles'] = v.groupby('vehicle type')['number of vehicles'].transform(lambda x: x / len(x))

    w2 = pd.DataFrame(dict(
        vehicle_type=v['vehicle type'],
        window_start=v['charge start'],
        window_end=v['charge end'],
        window_len=(v['charge end']-v['charge start']).mod(24),
        day_gal=v['gals fuel per year (2020)'] / days_in_reference_year,
        day_gal_2045=v['gals fuel per year (2045)'] / days_in_reference_year,
        day_kwh=v['kWh per year'] / days_in_reference_year,
        days_per_charge=1,
        charger_rating=v['charger rating (kW)'],
        charge_duration=(v['kWh per year']/days_in_reference_year/v['charger rating (kW)']),
        n_vehicles=v['number of vehicles'],
        energy_adjustment=1.0
    )).reindex(columns=cols)

    w = w.append(w2).reset_index(drop=True).copy()
    # assign vehicle ID
    w['veh_id'] = w.index
    # calculate total power used by each class of vehicle during charging, including adjustments
    w['charging_mw'] = w.eval(
        '0.001 * energy_adjustment * n_vehicles * charger_rating / days_per_charge'
    )
    # w[['vehicle_type', 'energy_adjustment', 'n_vehicles', 'charger_rating', 'days_per_charge', 'charging_mw']]



    ####################################
    # calculate vectors of charging power demand each timestep, for various price vectors

    bid_list = []
    for hours_per_step in ev_bid_step_sizes:
        # hours_per_step = 2
        assert(24 % hours_per_step == 0)
        prices = pd.DataFrame({'hour': range(0, 24, hours_per_step)})
        timesteps = get_timesteps_table(w, hours_per_step)
        min_price_hours = [None] + list(range(0, 24, hours_per_step))
        for bid_num, min_price_hour in enumerate(min_price_hours):
            # min_price_hour = 10
            if min_price_hour is None:
                # bid 0 uses flat pricing (business-as-usual charging)
                prices['price'] = 1
            else:
                # use distance of each hour from min_price_hour as a quasi-price,
                # to create a sawtooth price pattern (minimum at min_price_hour,
                # maximum 12 hours earlier/later)
                prices['price'] = prices.apply(
                    lambda r: min(abs(r['hour'] - min_price_hour), 24 - abs(r['hour'] - min_price_hour)),
                    axis=1
                )
            bid = get_ev_bid(timesteps, prices)
            bid['hours_per_step'] = hours_per_step
            bid['bid_number'] = bid_num
            bid_list.append(bid)
    bids = pd.concat(bid_list)
    # bids.groupby(['hours_per_step', 'bid_number']).size()

    # assign load zone (should be carried through from the start, but this is enough for now)
    bids['load_zone'] = vehicle_data.iloc[0, :]['load_zone']

    # save all the bid data to the database for later use
    bids.to_sql('ev_charging_bids', db_engine, if_exists='replace', index=False)
    # execute('ALTER TABLE ev_charging_bids OWNER TO admin;')
    execute('CREATE INDEX hh ON ev_charging_bids (hours_per_step, hour);') # may help a little
    # note: code above is a little slow; would be faster to create the table then use this:
    # execute("delete from ev_charging_bids;")
    # copy_dataframe_to_table(bids, 'ev_charging_bids')
    vehicle_data.columns = [c.replace('(', '').replace(')', '') for c in vehicle_data.columns]
    vehicle_data.to_sql('ev_fleet', db_engine, if_exists='replace', index=True)
    # execute('ALTER TABLE ev_fleet OWNER TO admin;')
    # show_index(vehicle_data.columns)

    # data stored in database:
    # ev_charging_bids:
    # bids for 1 or 2 hours-per-timestep, showing vectors of total electricity consumption
    # each timestep for 100% electric fleet, divided among vehicle classes.
    # ev_fleet:
    # total gasoline and diesel each year for 100% fossil fleet
    # capital cost recovery each year for incremental capital cost for a 100% electric fleet
    # then main model can choose fractionally between these and set costs accordingly

    print("Created ev_charging_bids and ev_fleet tables.")


def get_timesteps_table(w, hours_per_step):
    # calculate the timestep in which window_start falls (timesteps are also
    # counted in hours since midnight)
    # note: this code assumes timesteps start at midnight (no offset)
    w['window_start_timestep'] = (
        (w['window_start'] / hours_per_step).apply(np.floor) * hours_per_step
    )

    # create records for all possible charging timesteps for all vehicles,
    # indexed relative to the window_start_timestep.
    # note: this may include the same clock time twice for the same vehicle,
    # if the charging window is nearly 24 hours long,
    # e.g., if available at 10:45 am - 10:15 am, then the first window is
    # 10:00-11:00 and the last window is 10:00-11:00 the next day (which is
    # treated as 10:00-11:00 today, since it repeats from the previous day)

    n_chargesteps = 24 / hours_per_step + 1  # extra step in case last overlaps first
    timesteps = pd.DataFrame({
        col: w[col].repeat(n_chargesteps)
        for col in [
            'veh_id', 'vehicle_type',
            'window_start', 'window_end', 'window_len', 'window_start_timestep',
            'charge_duration', 'charging_mw'
        ]
    })
    # charge timesteps count in hours across the charging window (0 for the first timestep when
    # charging could occur, corresponding to window_start_timestep; clock time is
    # window_start_timestep + charge_timestep)
    timesteps['charge_timestep'] = np.tile(hours_per_step * np.arange(0, n_chargesteps), w.shape[0])
    timesteps['hour'] = (
        timesteps['window_start_timestep'] + timesteps['charge_timestep']
    ).astype(int).mod(24)

    # calculate number of hours of charging that could occur during each charge_timestep
    def chargeable_hours_in_step(row):
        """ calculate number of hours when charging could occur during each charge_timestep"""
        # calculate times relative to first charging timestep
        w_start = row['window_start'] - row['window_start_timestep']
        w_end = w_start + row['window_len']
        t_start = row['charge_timestep']
        t_end = t_start + hours_per_step
        # difference between end and start of chargeable time
        return min(max(w_end, t_start), t_end) - max(w_start, t_start)
    timesteps['chargeable_hours_in_step'] = timesteps.apply(chargeable_hours_in_step, axis=1)

    # look at vehicles with windows that might wrap around 24 hours
    # show_all(timesteps.query('window_len > 22'))

    # reduce size of timesteps table for efficiency
    timesteps = timesteps.loc[
        timesteps['chargeable_hours_in_step'] > 0,
        ['veh_id', 'vehicle_type', 'hour', 'charge_timestep', 'chargeable_hours_in_step', 'charge_duration', 'charging_mw']
    ]
    return timesteps

def get_ev_bid(timesteps, prices):
    """
    Accept a dataframe of per-timestep EV charging data `timesteps` and a dataframe of prices
    or price ranks `prices` for one day (which should be evenly spaced in integer-hour blocks),
    and return a vector of electricity demand for each time step of the day.
    """
    hours_per_step = int(24 / prices.shape[0])
    bid_index = pd.MultiIndex.from_product([timesteps['vehicle_type'].unique(), prices['hour']])

    # sort by vehicle, price rank, hours since start of window, then assign charging in order
    bid = timesteps.merge(prices, on='hour').sort_values(['veh_id', 'price', 'charge_timestep'])
    bid = bid.reset_index(drop=True).copy()

    # decide how much charging to do during each timestep
    # The commented-out code runs surprisingly slowly (a couple seconds for a
    # few hundred vehicles) and also has index matching errors because the group
    # label gets added to the index. So we just use the simple for loop below instead.
    # def charge_dur(g):
    #     dur_charged = g['chargeable_hours_in_step'].cumsum().clip(upper=g['charge_duration'])
    #     prev_charged = dur_charged.shift(1).fillna(0)
    #     return dur_charged - prev_charged
    # bid['charge_dur_in_timestep'] = bid.groupby('veh_id').apply(charge_dur) #.values
    charge_dur = []  # list of charge durations to apply to each row of bid frame
    prev_veh_id = None
    for r in bid.itertuples():
        if r.veh_id != prev_veh_id:
            # new vehicle, reset charge duration counter
            prev_veh_id = r.veh_id
            prev_dur = 0
        # charge as much as possible or as much as needed, whichever is less
        dur = min(r.chargeable_hours_in_step, r.charge_duration - prev_dur)
        prev_dur += dur
        charge_dur.append(dur)
    bid['charge_dur_in_timestep'] = pd.Series(charge_dur)

    bid['charge_mw'] = bid['charging_mw'] * bid['charge_dur_in_timestep'] / hours_per_step

    final_bid = bid.groupby(['vehicle_type', 'hour'])['charge_mw'].sum().reindex(bid_index).fillna(0)

    final_bid.index.names = ['vehicle_type', 'hour']
    final_bid = final_bid.reset_index()

    return final_bid


def fuel_costs():
    # create the fuel_costs table if needed
    execute("""
        CREATE TABLE IF NOT EXISTS fuel_costs (
            load_zone varchar(40),
            year int,
            month int, -- optional, NULL for annual, used for monthly series
            base_year int,
            fuel_type varchar(30),
            price_mmbtu float,
            fixed_cost float,
            max_avail_at_cost float,
            fuel_scen_id varchar(40),
            tier varchar(20),
            max_age int
        );
        -- ALTER TABLE fuel_costs OWNER TO admin;
    """)

    # note: most of the ones below are commented out to save time when re-running this
    # Also, most of them are obsolete (all except the Roberts ones and AEO 2018,
    # and maybe the 2016-11-22 ones if critiquing the PSIP).

    # TODO: add fixed_cost and max_avail_at_cost for EIA-based forecasts

    def eia_dir(*path):
        return data_dir('EIA-based fuel cost forecasts', *path)

    # Oahu fuel price forecasts, derived from EIA
    # import_eia_fuel_costs(eia_dir("HECO fuel cost forecasts.xlsx"), 'EIA_ref')
    # import_eia_fuel_costs(eia_dir("HECO fuel cost forecasts_low.xlsx"), 'EIA_low')
    # import_eia_fuel_costs(eia_dir("HECO fuel cost forecasts_high.xlsx"), 'EIA_high')
    # import_eia_fuel_costs(
    #     eia_dir("HECO fuel cost forecasts_LNG_pegged_to_oil.xlsx"), 'EIA_lng_oil_peg'
    # )
    # import_eia_fuel_costs(
    #     eia_dir("HECO fuel cost forecasts_high_LNG_pegged_to_oil.xlsx"), 'EIA_high_lng_oil_peg'
    # )

    # Oahu hedged fuel costs and equivalent unheged costs from HECO
    # (note: we used these instead of the PSIP Fuel Price Forecasts workbook because
    # these adjust to 2016 dollars and include LNG with various durations)
    # note: these are now superseded by the AEO 2018 prices.
    # import_hedged_fuel_costs(eia_dir("hedged fuel prices.xlsx"), tag='hedged')

    # print "importing hedged and unhedged fuel prices (2016-11-22)"
    # hedged_fuel_scen_id = 'hedged_2016_11_22'
    standard_fuel_scen_id = 'unhedged_2016_11_22'
    # import_hedged_fuel_costs(eia_dir("hedged fuel prices 2016-11-22.xlsx"), tag=hedged_fuel_scen_id)
    import_hedged_fuel_costs(
       eia_dir("unhedged fuel prices 2016-11-22.xlsx"), tag=standard_fuel_scen_id)

    # import_psip_fuel_costs(data_dir(
    #    "HECO Plans/PSIP-WebDAV/Resource Assumptions/"
    #    "PSIP Fuel Price Forecasts for HE 2016-06-27 regressions.xlsx"
    # ))

    # flat fuel price based on 2017 prices in 'unhedged_2016_11_22'
    execute("""
        CREATE TEMPORARY TABLE tfuelcosts AS
            SELECT * FROM fuel_costs WHERE fuel_scen_id=%s;
        UPDATE TFUELCOSTS a
            SET fuel_scen_id='flat_2016', price_mmbtu=b.price_mmbtu
            FROM tfuelcosts b
            WHERE b.year=2016 AND b.load_zone=a.load_zone AND b.fuel_type=a.fuel_type AND b.tier=a.tier;
        INSERT INTO fuel_costs SELECT * FROM tfuelcosts;
        DROP TABLE tfuelcosts;
    """, (standard_fuel_scen_id,))

    # print "importing Roberts fuel prices"
    # import_hedged_fuel_costs(eia_dir("Roberts EIA fuel prices 2018-02-15.xlsx"),
        # tag='roberts_eia_2018_02_15')
    # import_hedged_fuel_costs(eia_dir("Roberts futures fuel prices 2018-02-15.xlsx"),
        # tag='roberts_futures_2018_02_15')
    # import_hedged_fuel_costs(eia_dir("Roberts lower bound fuel prices 2018-02-15.xlsx"),
        # tag='roberts_lower_2018_02_15')

    # print("importing EIA AEO 2018 fuel prices")
    # import_hedged_fuel_costs(eia_dir("EIA AEO 2018 Reference Prices.xlsx"), tag='AEO_2018_Reference')
    # import_hedged_fuel_costs(
    #     eia_dir("EIA AEO 2018 High Oil Prices.xlsx"),
    #     tag='AEO_2018_High_Oil_Prices'
    # )
    # import_hedged_fuel_costs(eia_dir("EIA AEO 2018 Low Oil Prices.xlsx"), tag='AEO_2018_Low_Oil_Prices')

    print("importing EIA AEO 2019 fuel prices")
    files = [
        ("EIA AEO 2019 Reference Prices.xlsx", 'AEO_2019_Reference'),
        ("EIA AEO 2019 High Oil Prices.xlsx", 'AEO_2019_High_Oil_Prices'),
        ("EIA AEO 2019 Low Oil Prices.xlsx", 'AEO_2019_Low_Oil_Prices')
    ]
    for file, tag in files:
        import_hedged_fuel_costs(eia_dir(file), tag=tag)

    # print("importing monthly fuel prices")
    # import_monthly_fuel_costs(eia_dir('crude_oil_price_forecast_real_2019.csv'))

# def import_monthly_fuel_costs(file):
#     """
#     Notes from Hyun-gyu Kim, by e-mail 9/4/19:
#     The variable "high" indicates that the projection is based on EIA forecast,
#     "mid" is based on Brent crude oil futures prices and "low" is 1SD lower than
#     futures price. The price is converted to a real-term with 2019 as a based
#     year by using 2% inflation rate.
#     """
#
#     raise NotImplementedError(
#         "Need to import monthly historical prices from Hyun-gyu, with appropriate "
#         "fuel_scen_id into fuel_costs, including a month value. Then also need to "
#         "update code in scenario_data to crosslink the month value to particular "
#         "timepoints."
#     )
#
#     df = pd.read_csv(file)
#     df = df.set_index(['fuel_scen_id', 'year', 'fuel_type', 'tier']).sort_index()
#
#     for forecast in ['high', 'mid', 'low']:
#         tag = 'monthly_' + forecast
#         execute("DELETE FROM fuel_costs WHERE fuel_scen_id LIKE %s;", (tag,))
#         df.loc[''].to_sql('fuel_costs', db_engine, if_exists='append')
#
#         print("Added EIA-derived forecast (fuel_scen_id={}) to fuel_costs table.".format(fuel_scen_id))



def import_eia_fuel_costs(file, fuel_scen_id):

    # get the forecasts from an Excel workbook
    # Based on various sources, cited in the workbook, extended to 2050
    fuel_forecast = get_table_from_xlsx(file, named_range='Adjusted_EIA_Forecast', transpose=True)

    # note: all the EIA spreadsheets use a base year of 2013
    base_year = 2013

    # remove any existing records
    execute("""
        DELETE FROM fuel_costs WHERE fuel_scen_id=%s;
    """, (fuel_scen_id,))

    # take out the list of years, so the dictionary just has one entry for each fuel
    years = fuel_forecast.pop('Year')

    # insert data into the fuel_costs table
    n_rows = len(years)
    for f in fuel_forecast:
        ft=f.split(", ")
        fuel = ft[0]
        tier = ft[1] if len(ft) >= 2 else 'base'
        executemany("""
            INSERT INTO fuel_costs
                (load_zone, year, base_year, fuel_type, price_mmbtu, fuel_scen_id, tier)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            list(zip(['Oahu']*n_rows,
                years,
                [base_year]*n_rows,
                [fuel]*n_rows,
                fuel_forecast[f],
                [fuel_scen_id]*n_rows,
                [tier]*n_rows
            ))
        )

    print("Added EIA-derived forecast (fuel_scen_id={}) to fuel_costs table.".format(fuel_scen_id))


def import_hedged_fuel_costs(file, tag='hedged'):

    prices = data_frame_from_xlsx(file, named_range='fuel_prices')
    prices = prices.set_index(0)
    prices.index.name = 'year'
    prices = prices.T.set_index(['fuel_type', 'tier']).T.astype(float)
    # switch to one row per value, and assign a name to the value
    prices = pd.DataFrame({'price_mmbtu': prices.stack(['fuel_type', 'tier'])})
    prices['load_zone'] = 'Oahu'
    prices['base_year'] = get_named_cell_from_xlsx(file, named_range='base_year')

    tiers = data_frame_from_xlsx(file, named_range='tier_properties')
    # Transpose, set row and column labels, and convert to floating point (converting None to NaN)
    tiers = tiers.set_index(0).T.set_index(['fuel_type', 'tier']).astype(float)

    # fixed prices vary depending on the finance term; terms are pre-specified in this region
    fixed_costs = data_frame_from_xlsx(file, named_range='tier_fixed_costs')
    # use the first column as indexes (mostly to get column names), then set column headers
    fixed_costs = fixed_costs.set_index(0).T.set_index(['fuel_type', 'tier']).T
    # drop unneeded row for current finance term (we only want the values from the data table
    # below that)
    fixed_costs = fixed_costs.iloc[1:]
    # give the index a name
    fixed_costs.index.name = 'term'
    # convert to row-wise format, give the fixed_cost column a name, and convert the indexes to
    # columns
    fixed_costs = pd.DataFrame({'fixed_cost': fixed_costs.unstack()}).reset_index()
    # add a fuel_scen_id
    fixed_costs['fuel_scen_id'] = tag
    # use the term column as the maximum age for each tier with non-zero fixed costs
    limited_life = fixed_costs['fixed_cost'] > 0
    fixed_costs.loc[limited_life, 'max_age'] = fixed_costs.loc[limited_life, 'term']
    del fixed_costs['term']

    # remove duplicate rows (we don't need multiple rows with multiple ages for the $0 cost tiers)
    # also restore the indexes, to enable joining later
    fixed_costs = fixed_costs.drop_duplicates().set_index(['fuel_type', 'tier'])
    # merge the columns into the tiers table (adding all fuel_scen_id's and max_age's)
    tiers = tiers.join(fixed_costs)

    # merge the columns into the prices table (have to drop the year index to make this work)
    prices = prices.reset_index('year').join(tiers)

    # add the project lifespan into the tier id (have to convert tier index to a column to do this,
    # so might as well remove all indexes)
    prices = prices.reset_index()
    limited_life = prices['fixed_cost'] > 0
    prices.loc[limited_life, 'tier'] += (
        '_' + prices.loc[limited_life, 'max_age'].astype(int).astype(str).str.zfill(2)
    )

    # restore the indexes and sort the table
    prices = prices.set_index(['fuel_scen_id', 'year', 'fuel_type', 'tier']).sort_index()

    # remove any existing records
    execute("DELETE FROM fuel_costs WHERE fuel_scen_id LIKE %s;", (tag,))

    prices.to_sql('fuel_costs', db_engine, if_exists='append')

    print("Added hedged prices (fuel_scen_id = {}) to fuel_costs table.".format(
        list(prices.index.levels[0])
    ))

def import_psip_fuel_costs(file):
    # TODO: change this to do a more complete treatment of LNG options and coal
    # (not important immediately because we're just using this for "greenfield"
    # analysis of demand response)

    file = (
        data_dir +
        "/HECO Plans/PSIP-WebDAV/Resource Assumptions/"
        "PSIP Fuel Price Forecasts for HE 2016-06-27 regressions.xlsx"
    )
    fuel_scen_id = 'PSIP_2016_09'

    prices = data_frame_from_xlsx(file, named_range='real_fuel_prices').T.set_index(0).T
    year = data_frame_from_xlsx(file, named_range='years')

    prices = prices.set_index(year[0])
    prices = prices.astype(float)
    # drop unneeded columns and rename the remaining natural gas column
    del prices['ULSD']
    del prices['HECO LNG commodity']
    del prices['HECO LNG delivered']
    prices.rename(columns={'HG LNG delivered': 'LNG'}, inplace=True)

    # switch to one row per value, and assign a name to the value
    prices = pd.DataFrame({'price_mmbtu': prices.stack()})
    prices.index.rename(['year', 'fuel_type'], inplace=True)

    prices['load_zone'] = 'Oahu'
    prices['fuel_scen_id'] = fuel_scen_id
    prices['tier'] = 'base'
    prices['fixed_cost'] = 0
    prices['base_year'] = get_named_cell_from_xlsx(file, named_range='base_year')

    # remove any existing records
    execute("DELETE FROM fuel_costs WHERE fuel_scen_id like %s;", (fuel_scen_id,))

    prices.to_sql('fuel_costs', db_engine, if_exists='append')

    # reuse existing solid fuel data
    execute("""
        INSERT INTO fuel_costs
            SELECT load_zone, year, fuel_type, price_mmbtu,
                %s as fuel_scen_id, tier, fixed_cost, max_avail_at_cost, base_year
            FROM fuel_costs
            WHERE fuel_scen_id = 'EIA_ref' AND fuel_type in ('Coal', 'Pellet-Biomass');
    """, (fuel_scen_id,))

    # convert LNG to a 'bulk' tier and lookup relevant data
    execute("""
        UPDATE fuel_costs AS a
          SET tier = b.tier, fixed_cost = b.fixed_cost, max_avail_at_cost = b.max_avail_at_cost
          FROM fuel_costs b
          WHERE a.fuel_scen_id = %s AND a.fuel_type = 'LNG' AND a.tier = 'base'
              AND a.fuel_type = b.fuel_type
              AND b.fuel_scen_id = 'hedged_20' AND b.tier = 'bulk';
    """, (fuel_scen_id,))

    # # ULSD is not in the energy_source_properties database and isn't used in current scenarios
    # execute("DELETE FROM fuel_costs WHERE fuel_type = 'ULSD' AND fuel_scen_id = %s;",
    # (fuel_scen_id,))

    print("Added PSIP prices (fuel_scen_id = {}) to fuel_costs table.".format(fuel_scen_id))



#########################
# Fuel properties, maintained manually
def energy_source_properties():
    properties = get_table_from_xlsx(
        data_dir("EIA-based fuel cost forecasts", "Energy Source Properties.xlsx"),
        named_range='Fuel_Properties'
    )

    # create the fuel_properties table if needed
    execute("""
        CREATE TABLE IF NOT EXISTS energy_source_properties (
            energy_source VARCHAR(30) PRIMARY KEY,      -- name of the fuel
            fuel_rank DECIMAL(4, 2),           -- usually 1-5, but may be decimal, e.g., 1.5
            rps_eligible SMALLINT,             -- 0 or 1
            co2_intensity FLOAT                -- tCO2 per MMBtu
        );
    """)

    # create a temporary table to hold the data before aggregating by fuel type
    execute("""
        DROP TABLE IF EXISTS t_energy_source_properties;
        CREATE TEMPORARY TABLE t_energy_source_properties (LIKE energy_source_properties);
    """)

    # insert data into the energy_source_properties table
    executemany("""
        INSERT INTO t_energy_source_properties (energy_source, fuel_rank, rps_eligible, co2_intensity)
        VALUES (%s, %s, %s, %s)""",
        list(zip(
            [f.split(', ')[0] for f in properties['Fuel']],
            properties['Rank'],
            properties['RPS Eligible'],
            [i/1000.0 for i in properties['kg CO2 per MMbtu']],
        ))
    )

    # move the data into the main energy_source_properties table
    execute("""
        DELETE FROM energy_source_properties;
        INSERT INTO energy_source_properties SELECT DISTINCT * FROM t_energy_source_properties;
        DROP TABLE t_energy_source_properties;
    """)

    print("Created energy_source_properties table.")

def fuel_costs_no_biofuel():
    """Create no-biofuel fuel cost scenarios"""
    # note: these are not used anymore; the same effect can be achieved by setting
    # '--biofuel-limit 0'
    execute("""
        DELETE FROM fuel_costs WHERE fuel_scen_id LIKE 'EIA_%_no_biofuel';
        DROP TABLE IF EXISTS t_fuel_costs_no_biofuel;
        CREATE TABLE t_fuel_costs_no_biofuel AS
        SELECT c.*
            FROM fuel_costs c JOIN energy_source_properties p ON c.fuel_type = p.energy_source
            WHERE rps_eligible = 0 AND fuel_scen_id LIKE 'EIA_%';
        UPDATE t_fuel_costs_no_biofuel SET fuel_scen_id = fuel_scen_id || '_no_biofuel';
        INSERT INTO fuel_costs SELECT * FROM t_fuel_costs_no_biofuel;
        DROP TABLE t_fuel_costs_no_biofuel;
    """)

def onshore_wind():
    """Import old onshore wind data into current tables."""
    # TODO: write code to create these records directly from OWITS data and GIS
    # files and also store location and interconnect distance
    # Note: these include new and existing wind farms, and have latitude
    # and longitude for existing ones (only), so they can be cross-matched
    # to existing project definitions later.
    wind_dir = data_dir('Resource Assessment', 'Wind', 'owits_results')
    wind_proj = pd.read_csv(os.path.join(wind_dir, 'wind_project.csv'))
    wind_cap_factor = pd.read_csv(os.path.join(wind_dir, 'wind_cap_factor.csv'))

    print("Copying definitions for onshore wind farms into project and cap_factor tables (abt. 2 mins)")
    # drop old records (project and cap factor tables should already exist)
    execute("""
        delete from cap_factor
            where project_id in
                (select project_id FROM project where technology = 'OnshoreWind');
        delete FROM project where technology='OnshoreWind';
    """)

    # add project data
    wind_proj.loc[:, [
        'load_zone', 'technology', 'site', 'orientation',
        'max_capacity', 'latitude', 'longitude',
        'connect_distance_km'  # legacy distance
    ]].to_sql('project', con=db_engine, if_exists='append', index=False)

    # retrieve new project_ids, generated in database
    wind_proj = wind_proj.rename({'project_id': 'old_project_id'}, axis=1)
    project_ids = pd.read_sql(
        "SELECT project_id, load_zone, technology, site, orientation "
        + "FROM project WHERE technology='OnshoreWind';",
        db_engine
    )
    wind_proj = wind_proj.merge(
        project_ids,
        on=['load_zone', 'technology', 'site', 'orientation']
    ).set_index('old_project_id')

    # update cap_factor project_ids
    wind_cap_factor = (
        wind_cap_factor
        .rename({'project_id': 'old_project_id'}, axis=1)
        .set_index('old_project_id')
    )
    wind_cap_factor['project_id'] = wind_proj['project_id']
    shared_tables.drop_indexes('cap_factor')
    copy_dataframe_to_table(wind_cap_factor, 'cap_factor')
    shared_tables.create_indexes('cap_factor')


def offshore_wind():
    """Import capacity factor for offshore wind farms. This is calculated as the
    average of three proposed offshore sites to get approximately the right amount
    for diversified offshore wind. (It might be better just to model them as three
    separate projects.)

    Note: The 2016 PSIP used hourly output possibly for 2014, from an existing
    wind farm on the Big Island with a capacity factor of 42%. We don't use this
    because it's the wrong profile for offshore Oahu, and especially because it
    has inconsistent timing with our other weather and load data so it would
    create an artificial appearance of diversity (strong winds when Oahu actually
    has windless/sunless days).
    """
    # approximate locations for the centers of three proposed wind farms
    # were found on 2016-04-07 by inspecting the
    # "Atlantic and Pacific OCS Wind Planning Areas and Wind Energy Areas"
    # shapefile from http://www.boem.gov/Renewable-Energy-GIS-Data/
    # (http://www.boem.gov/uploadedFiles/BOEM/Renewable_Energy_Program/
    # Mapping_and_Data/Wind_Planning_Areas.zip)
    locs = np.array([[21.656, -158.572], [21.096, -157.987], [20.969, -157.799]])
    # cells: array with one row per cell, cols are i, j, lat, lon

    # before 10/7/21 (now crashed): owits_root = 'http://redr.eng.hawaii.edu:8888'
    owits_root = data_dir('Resource Assessment', 'OWITS')

    cells = pd.read_csv(owits_root + '/OWITS/E_Georef.csv').values

    cell_lat_lon = cells[:,-2:]
    # this makes one row for each site, one col for each cell, showing distance in degrees**2
    dist2 = ((locs[:,np.newaxis,:] - cell_lat_lon[np.newaxis,:,:])**2).sum(axis=2)
    match_cells = dist2.argmin(axis=1)
    # turbine_cells: array with one row per offshore site, cols are i, j, lat, lon
    turbine_cells = cells[match_cells]

    # normalized power curve for generic offshore wind turbine from
    # http://www.nrel.gov/docs/fy14osti/61714.pdf p. 5,
    # with operating range extended to 30 m/s like Repower 6 M shown on p. 4.
    # one row per point on curve, first col is wind speed, second is cap factor
    power_curve = np.array(list(zip(
        list(range(32)),
        [0] * 4 + [
            0.0281, 0.074, 0.1373, 0.2266, 0.3443, 0.4908,
            0.6623, 0.815, 0.9179, 0.9798
        ] + [1] * 17 + [0]
    )))

    # read 10-min wind speed data for each wind farm site, convert to production,
    # average across hour. Then use average across sites as a single project.
    if owits_root.startswith("http"):
        print("Retrieving hourly offshore wind data from {} (abt. 1 min).".format(owits_root))
    hourly = 0 # will become a series with one row per historical hour
    for i, j, lat, lon in turbine_cells:
        ten_min = pd.read_csv(
            owits_root + '/OWITS_DATA/E/{:04g}_{:04g}.HAWAII.E.txt'.format(i, j),
            # from http://redr.eng.hawaii.edu:8888/OWITS/README_OWITS_DATA.TXT
            names=
                'DATE,TIME,TSFC,PSFC,PCP,Q2M,DSWRF,DLWRF,T10,S10,W10,T50,S50,'
                'W50,T80,S80,W80,T100,S100,W100,T200,S200,W200'.split(',')
        )
        ten_min['date_time'] = pd.to_datetime(
            ten_min['DATE'] * 10000 + ten_min['TIME'],
            format='%Y%m%d%H%M'
        ).dt.tz_localize('UTC').dt.tz_convert('HST').dt.floor('h')
        # Calculate capacity factor, derating for losses same as we do for
        # onshore sites (from IRP 2013)
        ten_min['cap_factor'] = np.interp(
            ten_min['S100'].values, power_curve[:,0], power_curve[:,1]
        ) * 0.8747
        hourly += ten_min.groupby('date_time')['cap_factor'].mean()/len(turbine_cells)

    print("storing offshore wind data in database (abt 1 min.)")
    shared_tables.drop_indexes('cap_factor')
    # delete any old OffshoreWind records from cap_factor
    execute("""
        delete from cap_factor
            where project_id in
                (select project_id FROM project
                    where load_zone = 'Oahu' and technology = 'OffshoreWind');
    """)

    # add the new project to the project table
    execute("""
        delete from project where technology = 'OffshoreWind' and load_zone = 'Oahu';
        insert into project
            (load_zone, technology, site, orientation, max_capacity)
            values ('Oahu', 'OffshoreWind', 'OffWind', 'na', 2400);
    """)
    # retrieve the project_id for the new project
    project_id = next(execute("""
        select project_id FROM project where load_zone = 'Oahu' and technology = 'OffshoreWind';
    """))[0]

    # put the power data into cap_factor
    # convert hourly to a dataframe, with date_time as a column
    hourly = hourly.reset_index()
    hourly['project_id'] = project_id
    # old code, no longer seems to be needed:
    # convert time to string to force use of correct time zone
    # hourly['date_time'] = hourly['date_time'].dt.strftime("%Y-%m-%d %H:%M:%S%z")
    copy_dataframe_to_table(hourly, 'cap_factor')
    shared_tables.create_indexes('cap_factor')

    # note: we don't add latitude, longitude or interconnect_id (and cost) because we don't
    # have project-specific connection costs for them. So they will automatically use
    # the generic connection cost from generator_info (assigned later).
    # That happens to be zero in this case since the connection cost is included in the overnight cost.


def renewable_supply_curve():
    """
    Save renewable energy supply curve for later graphing.
    See "<gis_dir>/renewable energy supply curve.xlsx" for final graphs.
    """
    supply_curve = pd.read_sql(
        """
            select
                -- consolidate DistPV technologies
                REPLACE(REPLACE(technology, 'FlatDistPV', 'DistPV'), 'SlopedDistPV', 'DistPV') AS technology,
                max_capacity,
                sum(max_capacity*cap_factor)/sum(max_capacity) as cap_factor
            from project p
            join cap_factor c using (project_id)
            group by p.project_id, 1, 2
            order by 1, 3 desc;
        """,
        db_engine
    )
    # add the first step on each part of the supply curve (0 cumulative MW)
    first_points = supply_curve.groupby('technology')[['cap_factor']].max().reset_index()
    first_points['max_capacity'] = 0
    supply_curve = (
        pd.concat([supply_curve, first_points], axis=0, sort=False)
        .sort_values(
            ['technology', 'cap_factor', 'max_capacity'],
            axis=0, ascending=[True, False, True]
        )
    )
    supply_curve['cumulative_mw'] = supply_curve.groupby('technology')['max_capacity'].cumsum()
    # put the columns in the right order for plotting
    supply_curve = supply_curve[['technology', 'cumulative_mw', 'cap_factor']]

    gis_dir = data_dir('Resource Assessment', 'GIS')
    supply_curve.to_csv(os.path.join(gis_dir, 're_supply_curve.csv'), index=False)


def generator_info():
    # note: these must always be run in this sequence, because
    # new_generator_info() creates the generator_info and part_load_fuel_consumption
    # tables, and then existing_generator_info() appends to them without clearing
    # out any prior records.
    # shared_tables.create_table('project')  # renewable project definitions carry over
    new_generator_info()
    existing_generator_info() # depends on new_generator_info to create the generator_info table

# def sub_df(df, top_left=[0, 0], index_cols=0, header_rows=0):
#     """
#     Return subsection of df, starting at specified location and extending right
#     and down until no data are present, using specified number of columns as
#     index and specified number of rows as column labels.
#     This is useful for selecting subsections out of a spreadsheet page that has
#     been read into a dataframe
#     """
#     # find the edge of the data (assumed to be limited by extent of data in first
#     # row and column)
#     last_index = lambda series: series.index.get_loc(series.last_valid_index())
#     data_top = top_left[0] + header_rows
#     data_left = top_left[1] + index_cols
#     last_row = last_index(df.iloc[data_top:, data_left]) + data_top
#     last_col = last_index(df.iloc[data_top, data_left:]) + data_left
#     raise NotImplementedError("Still need to extract data section and set index and columns.")
#
# def atb_generator_info():
#     """Read data on renewable generation technologies from NREL Annual Technology Baseline"""
#     storage = pd.read_excel(atb_file, 'Storage')
#     top_left = np.argwhere(storage.values == 'Battery Pack Capital Cost ($/kWh)')[0] + [1, 0]
#     df = sub_df(storage, top_left, index_cols=1, index_rows=1)
#     ...

def new_generator_info():
    """
    Read data from technology_data_file and store it in generator_info and
    generator_costs_by_year.
    """

    base_years = data_frame_from_xlsx(technology_data_file, 'cost_base_years')
    base_years = base_years.set_index(0).T.set_index(['technology', 'tech_scen_id'])

    gen_info = data_frame_from_xlsx(technology_data_file, 'technology_info')
    # set column headers and row indexes (index in the dataframe become index in the table)
    gen_info = gen_info.T.set_index(0).T.set_index(['technology', 'tech_scen_id'])
    gen_info.rename(
        columns={
            'variable_o_m_per_mwh': 'variable_o_m',
            'full_load_heat_rate': 'heat_rate'
        }, inplace=True)
    # convert from cost per MWh to cost per kWh
    gen_info['variable_o_m'] *= 0.001

    # # convert unit_size = NA or 0 to NaN (always NA now, handled below)
    # gen_info['unit_size'] = gen_info['unit_size'].where(
    #     (gen_info['unit_size'] != "NA") & (gen_info['unit_size'] != 0)
    # )
    # report base_year for inflation calculations later
    gen_info['base_year'] = base_years.loc[gen_info.index, 'cost_base_year']

    # convert all columns except fuel to numeric values, replacing
    # non-numeric values (e.g., "NA") with nan.
    # gen_info.convert_objects() does this nicely, but is deprecated.
    for c in gen_info.columns:
        if c not in {'fuel'}:
            gen_info[c] = pd.to_numeric(gen_info[c], errors='coerce')

    # add flat-cost scenario
    gen_info_flat = gen_info.loc[(slice(None), flat_base_scenario), :]
    gen_info_flat.index.set_levels(
        gen_info_flat.index.levels[1].str.replace(flat_base_scenario, flat_scenario),
        level=1, inplace=True
    )
    gen_info = pd.concat([gen_info, gen_info_flat], axis=0)

    # drop and recreate generator_info table (records for existing technologies will be
    # created in existing_generator_info)
    print("creating generator_info table with data for future generation technologies.")
    execute('DROP TABLE IF EXISTS generator_info;')
    shared_tables.create_table('generator_info')  # defines some columns that aren't in gen_info
    gen_info.to_sql('generator_info', db_engine, if_exists='append')

    # load gen capital cost info
    def get_technology_cost_series(region):
        # read standard info frame from specified region in technology_data_file,
        # setting column headers and index appropriately (should be labeled as
        df = data_frame_from_xlsx(technology_data_file, region)
        df = df.T.set_index([0, 1]).T
        df.columns.names = df.iloc[:,0].name  # should be 'technology', 'tech_scen_id'
        df = df.set_index(df.iloc[:,0].name)
        df.index.name = 'year'
        return df.unstack()

    gen_costs = pd.DataFrame({
        'capital_cost_per_kw': get_technology_cost_series('technology_cap_cost'),
        'capital_cost_per_kwh': get_technology_cost_series('technology_cap_cost_energy'),
        'fixed_o_m': get_technology_cost_series('technology_fixed_o_m'),
    })
    # record the base year to allow adjustment to other years later
    gen_costs = gen_costs.join(base_years, how='left') \
        .rename(columns={'cost_base_year': 'base_year'})

    # make extra records with flat costs all the way through
    # start with standard data
    gen_costs_flat = gen_costs.loc[(slice(None), flat_base_scenario, slice(None)), :]
    # get current values, with no year index
    gen_costs_ref_year = gen_costs_flat.loc[(slice(None), slice(None), flat_ref_year), :].reset_index('year', drop=True)
    # remake gen_costs_flat with those values, via join with no columns or year
    gen_costs_flat = gen_costs_flat.loc[:,[]].reset_index('year').join(gen_costs_ref_year).set_index('year', append=True)
    gen_costs_flat.index.set_levels(
        gen_costs_flat.index.levels[1].str.replace(flat_base_scenario, flat_scenario),
        level=1, inplace=True
    )
    gen_costs = pd.concat([gen_costs, gen_costs_flat], axis=0)
    # convert columns to numeric types, converting any missing/text values (e.g., '#N/A') to NaN
    for col in gen_costs.columns:
        gen_costs[col] = pd.to_numeric(gen_costs[col], errors='coerce')
    # store costs in database
    print("creating generator_costs_by_year table with data for future generation technologies.")
    gen_costs.to_sql('generator_costs_by_year', db_engine, if_exists='replace')

    # import part-load heat rates
    gen_fuel_cons = data_frame_from_xlsx(technology_data_file, 'part_load_fuel_consumption')
    gen_fuel_cons = gen_fuel_cons.T.set_index(0).T
    gen_fuel_cons = gen_fuel_cons.rename(columns={
        'load level (MW)': 'output_mw',
        'fuel consumption (MMBtu/h)': 'fuel_consumption_mmbtu_per_h',
    }).set_index('technology')
    print("creating part_load_fuel_consumption table with data for future generation technologies.")
    gen_fuel_cons.to_sql('part_load_fuel_consumption', db_engine, if_exists='replace')

    #############
    # import definitions for non-renewable/non-resource-limited projects
    # We could just construct these in scenario_data.py from the generator_info entries,
    # except that we need to specify a maximum capacity for each project to support
    # the RPS calculation (which uses that in a big-M constraint that allocates output
    # among fuels)
    project_info = data_frame_from_xlsx(technology_data_file, 'non_renewable_project_info') \
        .T.set_index(0) \
        .T.set_index(['load_zone', 'technology', 'site', 'orientation'])
    # remove all non-renewable project definitions from project table before inserting new ones
    execute(
        'DELETE FROM project WHERE technology NOT IN %s',
        [tuple(renewable_techs)]
    )
    # insert non-renewable project definitions into project table

    project_info.to_sql('project', db_engine, if_exists='append')

    # add DistBatteries option
    distributed_batteries()


def distributed_batteries():
    """
    Add DistBattery technology identical to Battery_Bulk; used to
    distinguish forecasted, distributed batteries from optimized utility-scale
    batteries in some studies.
    """
    for table in ['generator_info', 'generator_costs_by_year', 'project']:
        df = pd.read_sql(
            "SELECT * FROM {} WHERE technology='Battery_Bulk';".format(table),
            con=db_engine
        )
        df['technology'] = 'DistBattery'
        if table == 'project':
            # Avoid creating duplicate keys
            del df['project_id']
        df.to_sql(table, db_engine, index=False, if_exists='append')
    print("Added DistBattery technology to database.")


def existing_generator_info():
    """copy data from 'Data/Generator Info/Existing Plant Data.xlsx' into
    generator_info, part_load_fuel_consumption, project and proj_existing_builds
    """
    # NOTE: new_generator_info() must also be called each time this is run

    gen_info_file = data_dir('Generator Info', 'Existing Plant Data.xlsx')

    ################
    # create generator technology definitions for existing projects that weren't
    # covered by new_generator_info() (these are all the existing thermal plants
    # except Schofield)
    gen_info = data_frame_from_xlsx(gen_info_file, 'technology_info') \
        .T.set_index(0).T.set_index('technology')

    # convert from cost per MWh to cost per kWh
    gen_info['variable_o_m'] *= 0.001

    # add some fields
    gen_info['min_vintage_year'] = gen_info['build_year']
    gen_info['max_age_years'] = gen_info['retirement year'] - gen_info['build_year']
    gen_info['base_year'] = get_named_cell_from_xlsx(gen_info_file, named_range='base_year')
    gen_info['tech_scen_id'] = 'all' # reuse plant definitions for all scenarios

    # keep only basic generator info (dropping project-related fields)
    gen_info = gen_info.loc[:, 'unit_size':]

    # store generator info
    # note: the table should have been emptied and recreated by new_generator_info()
    # before calling this function
    print("Adding records for existing thermal plants to generator_info table.")
    gen_info.to_sql('generator_info', db_engine, if_exists='append')

    ################
    # create heat rate curves
    heat_rate_curves = data_frame_from_xlsx(gen_info_file, 'heat_rate_info')
    # place dummy values in the first level of the index; otherwise NaNs match any slice
    heat_rate_curves.loc[0, :] = heat_rate_curves.loc[0, :].fillna('x')
    # create the column index
    heat_rate_curves = heat_rate_curves.T.set_index([0, 1]).T
    # create the row index
    heat_rate_curves = heat_rate_curves.set_index(('x','technology'))
    heat_rate_curves.index.names=['technology']
    # get heat-rate specific info
    heat_rate_curves = heat_rate_curves[['PP', 'FC']].rename(
        columns={'PP':'output_mw', 'FC': 'fuel_consumption_mmbtu_per_h'}
    ).astype(float)

    # switch to database format
    heat_rate_curves = heat_rate_curves.stack()[['output_mw', 'fuel_consumption_mmbtu_per_h']]
    # don't use min/1/2/3/max labels
    heat_rate_curves.index = heat_rate_curves.index.droplevel(1)
    # sort rows appropriately (only matters for display)
    heat_rate_curves = heat_rate_curves.reset_index()
    heat_rate_curves = heat_rate_curves.sort_values(
        ['technology', 'output_mw', 'fuel_consumption_mmbtu_per_h']
    )
    heat_rate_curves = heat_rate_curves.set_index('technology')
    # drop blank entries and treat the rest as floating point
    heat_rate_curves = heat_rate_curves.astype(float).dropna(axis=0, subset=['output_mw'])

    # store in database (should already be emptied and created by new_generator_info())
    print("Adding records for existing thermal plants to part_load_fuel_consumption table.")
    heat_rate_curves.to_sql('part_load_fuel_consumption', db_engine, if_exists='append')

    ################
    # create definitions for existing projects that can't be extended in the future
    # (not handled elsewhere).
    # note: new_generator_info() defines thermal projects that can be extended and the wind
    # and tracking_pv code define renewable projects (all based on the technology_data_file).
    projects = data_frame_from_xlsx(gen_info_file, 'technology_info').T.set_index(0).T

    # create columns not provided in xlsx file
    projects['max_capacity'] = projects['proj_existing_cap']

    # set index and choose correct columns for database
    projects = projects.set_index(['load_zone', 'technology', 'site', 'orientation'])
    projects = projects[[
        'max_capacity', 'latitude', 'longitude',
    ]]
    # insert definitions into project table
    print("Adding records for existing thermal plants to project table.")
    projects.to_sql('project', db_engine, if_exists='append')

    ##############
    # create proj_existing_builds, holding construction dates and costs for existing and
    # planned projects; this assigns solar projects to suitable resource tranches, which
    # automatically reduces the amount of those tranches available for new construction.
    # Note: costs should be left blank for planned projects if costs are provided for the
    # build_year in the technology_data_file.
    proj_build = data_frame_from_xlsx(gen_info_file, 'build_info').T.set_index(0).T

    # assign existing utility-scale renewable energy projects to the nearest site
    near_query = """
        select
            site, orientation,
            ((latitude-%(latitude)s)^2+(longitude - %(longitude)s)^2)^0.5 as dist
        from project
        where load_zone=%(load_zone)s and technology=%(technology)s
        order by 3
        limit 1;
    """
    for row in proj_build.itertuples():
        if row.technology in renewable_techs and row.technology not in {'FlatDistPV', 'SlopedDistPV'}:
            # find the nearest project and assign this capacity to that
            # note: row is a namedtuple; vars() should convert it to a dict but doesn't work on Python 3.7
            # https://docs.python.org/3.3/library/collections.html#collections.somenamedtuple._asdict
            nearest = pd.read_sql(sql=near_query, con=db_engine, params=row._asdict())
            proj_build.loc[row.Index, ['site', 'orientation']] = nearest.loc[0, ['site', 'orientation']]

    # replace single FlatDistPV and SlopedDistPV projects with several projects
    # spread among the better-than-average resources within that zone (e.g., south-facing roofs)

    # technology = 'FlatDistPV'
    for technology in ['FlatDistPV', 'SlopedDistPV']:
        # remove the DistPV rows from proj_build and keep them for further reference
        proj_build_dist_pv = proj_build[proj_build['technology']==technology]
        proj_build = proj_build.drop(proj_build_dist_pv.index)

        # get a list of all better-than-average solar sites in each zone
        dist_pv_tranche_query = """
            WITH site_cap_factor AS (
                SELECT
                    load_zone, technology, site, orientation, max_capacity as site_capacity,
                    AVG(cap_factor) AS cap_factor
                FROM project JOIN cap_factor USING (project_id)
                WHERE technology='{}'
                GROUP BY 1, 2, 3, 4, 5
            ), zone_cap_factor AS (
                SELECT
                    load_zone, technology,
                    sum(cap_factor*site_capacity)/SUM(site_capacity) AS zone_cap_factor
                    FROM site_cap_factor
                    GROUP BY 1, 2
            ), good_sites as (
                SELECT *
                FROM site_cap_factor s JOIN zone_cap_factor z USING (load_zone, technology)
                WHERE cap_factor >= zone_cap_factor
            ), zone_good_capacity AS (
                SELECT load_zone, technology, SUM(site_capacity) AS zone_good_capacity
                FROM good_sites
                GROUP BY 1, 2
            )
            SELECT *
            FROM good_sites JOIN zone_good_capacity USING (load_zone, technology);
        """.format(technology)
        dist_pv_tranches = pd.read_sql(dist_pv_tranche_query, con=db_engine)

        # pair project templates with tranches based on load_zone and technology
        # (but not site or orientation)
        new_rows = (
            proj_build_dist_pv.drop(['site', 'orientation'], axis=1) \
            .merge(dist_pv_tranches, on=['load_zone', 'technology'], how='left')
        )
        # allocate existing capacity among tranches
        new_rows['proj_existing_cap'] = (
            new_rows['proj_existing_cap'] * new_rows['site_capacity'] / new_rows['zone_good_capacity']
        )
        # append matching columns to proj_build
        proj_build = proj_build.append(new_rows.reindex(columns=proj_build.columns))

    # lookup project_id's for existing projects
    proj_id = pd.read_sql(
        'SELECT project_id, load_zone, technology, site, orientation FROM project;',
        con=db_engine
    )
    proj_build = proj_build.merge(proj_id, how='left')
    proj_unmatched = proj_build[proj_build['project_id'].isnull()]
    if proj_unmatched.shape[0] > 0:
        print("="*70)
        print("WARNING: The following existing projects were not found in the project table:")
        print(proj_unmatched)
        print('See "{}" for details.'.format(gen_info_file))
        print("="*70)

    # create/replace proj_existing_builds table (with appropriate formats for columns)
    proj_build['build_year'] = proj_build['build_year'].astype(int)
    proj_build = proj_build.set_index(['project_id', 'build_year'])
    proj_build = proj_build[['proj_existing_cap', 'proj_overnight_cost', 'proj_fixed_om']].astype(float)
    proj_build['base_year'] = get_named_cell_from_xlsx(gen_info_file, named_range='base_year')
    print("Adding projects to proj_existing_builds table.")
    proj_build.to_sql('proj_existing_builds', db_engine, if_exists='replace')

    # make sure no projects are over-allocated
    # (may also prompt an error or infeasibility in SWITCH later)
    # TODO: use a moving window (e.g., 20 years for DistPV) to account for retirements
    excess_allocation = pd.read_sql(
        """
            SELECT
                project.project_id, load_zone, technology, site, orientation, max_capacity,
                sum(proj_existing_cap) as proj_existing_cap
            FROM project JOIN proj_existing_builds USING (project_id)
            GROUP BY 1, 2, 3, 4, 5, 6
            HAVING sum(proj_existing_cap) > max_capacity;
        """,
        con=db_engine
    ).set_index('project_id')
    if excess_allocation.shape[0] > 0:
        print("="*70)
        print("WARNING: The following projects have installations greater than")
        print("the maximum possible capacity:")
        print(excess_allocation)
        print('See "{}" for details.'.format(gen_info_file))
        print("="*70)


def historical_loads():
    ferc_respondents = pd.read_csv(ferc714_respondent_file, encoding='latin1')
    heco_respondent_id = ferc_respondents.loc[
        ferc_respondents['plan_area_name'].str.startswith("Hawaiian Electric Company"),
        'respondent_id'
    ].min()
    ferc = (
        pd.read_csv(ferc714_load_file)
        .query('respondent_id == {}'.format(heco_respondent_id))
    )
    # Use time zones from data file, then convert to HST.
    ferc['plan_date'] = pd.DatetimeIndex(ferc['plan_date'])
    ferc['day_start'] = (
        ferc
        .groupby('timezone')['plan_date']
        .transform(lambda x: x.dt.tz_localize(x.name))
    )
    # As noted below, this code doesn't work in Pandas 0.24.2
    from pkg_resources import parse_version
    if parse_version(pd.__version__) < parse_version('0.25'):
        # Note: for some reason on pandas 0.24.2 .transform() converts back to naive datetime,
        # at which point it shifts to UTC, so then we have to tell it this is UTC
        # and then convert back to HST. This is fixed later.
        ferc['day_start'] = ferc['day_start'].dt.tz_localize('UTC')
    # Note: This may break if ferc day_start has mixed timezones
    ferc['day_start'] = ferc['day_start'].dt.tz_convert('HST')
    ferc = ferc.set_index('day_start').loc[:, 'hour01':'hour25']
    ferc.columns = range(25)  # switch to zero-indexed int hours
    ferc.columns.name = 'hour'
    ferc = ferc.stack().to_frame(name='net_load').reset_index()
    ferc['date_time'] = ferc['day_start'] + ferc['hour'] * pd.Timedelta(hours=1)
    ferc['load_zone'] = 'Oahu'
    # For Hawaii, we can just drop hour 25, because it is never used.
    # For other regions, we would need to investigate how the file handled
    # daylight saving time (one missing hour at start, extra hour at end)
    ferc = ferc.loc[ferc['hour'] < 24, ['load_zone', 'date_time', 'net_load']]

    tz = ferc['date_time'].dt.tz

    load = ferc

    # note: later versions of this script calculate gross system_load
    # as net load (from FERC) plus DER generation minus EV charging, but
    # we did not do that for this project.
    load['system_load'] = load['net_load']

    # bulk copy to database (~100x faster than to_sql)
    # first (re-)create the empty table:
    load.loc[[], :].to_sql(
        'system_load', con=db_engine, index=False, if_exists='replace'
    )
    # convert time to string to force use of correct time zone
    load['date_time'] = load['date_time'].dt.strftime("%Y-%m-%d %H:%M:%S%z")
    copy_dataframe_to_table(load, 'system_load')
    # Ensure uniqueness and possibly help with queries
    execute("ALTER TABLE system_load ADD PRIMARY KEY (load_zone, date_time);")


def system_load():
    # TODO: extend to other load zones by adding more rows to the
    # 'sales_forecast' region of the technology_data_file

    historical_loads()

    # get historical peak and average loads
    hist = pd.read_sql(
        sql="""
            SELECT
                load_zone, EXTRACT(year FROM date_time) as year_hist,
                MAX(system_load) as peak_hist, AVG(system_load) as avg_hist
            FROM system_load
            GROUP BY 1, 2;
        """,
        con=db_engine
    )
    # forecast peak and energy
    fore = data_frame_from_xlsx(technology_data_file, 'sales_forecast')
    fore = fore.T.set_index(0).T
    fore = fore.rename(columns={'year': 'year_fore'})
    # calculate scale factors for system_load_scale table
    sls = pd.merge(hist, fore, on='load_zone')
    sls['load_scen_id'] = load_scen_id
    sls['peak_fore'] = sls['underlying forecast (MW)'] + sls['energy efficiency (MW)']
    sls['avg_fore'] = (sls['underlying forecast (GWh)'] + sls['energy efficiency (GWh)'])/8.76
    sls['scale'] = (sls['peak_fore'] - sls['avg_fore']) / (sls['peak_hist'] - sls['avg_hist'])
    sls['offset'] = sls['peak_fore'] - sls['scale'] * sls['peak_hist']

    # put into standard order, drop unneeded columns, convert to the right types for the database
    db_columns = [
        'load_zone', 'load_scen_id', 'year_hist', 'year_fore',
        'peak_hist', 'peak_fore', 'avg_hist', 'avg_fore', 'scale', 'offset'
    ]
    system_load_scale = pd.DataFrame()
    for c in db_columns:
        if c in ['load_zone', 'load_scen_id']:
            system_load_scale[c] = sls[c].astype(str)
        elif c in ['year_hist', 'year_fore']:
            system_load_scale[c] = sls[c].astype(int)
        else:
            system_load_scale[c] = sls[c].astype(float)
    system_load_scale.set_index(db_columns[:4], inplace=True)
    # store data
    # execute("DELETE FROM system_load_scale WHERE load_scen_id=%s;", (load_scen_id,))
    system_load_scale.to_sql('system_load_scale', db_engine, if_exists='replace')

    # create another forecast with peak and average loads from 2007, carried through to the future
    execute("""
        CREATE TEMPORARY TABLE tsls (LIKE system_load_scale);
        INSERT INTO tsls
            (load_scen_id, load_zone, year_fore, peak_fore, avg_fore,
            year_hist, peak_hist, avg_hist)
            SELECT
                'flat_2007' as load_scen_id, slf.load_zone,
                year_fore, peak_fore, avg_fore,
                year_hist, peak_hist, avg_hist
            FROM (
                -- generate list of years for potential studies
                SELECT GENERATE_SERIES(2007, 2050) as year_fore
            ) years
            CROSS JOIN (
                -- use 2007 loads as forecast
                SELECT
                    load_zone,
                    MAX(system_load) as peak_fore,
                    AVG(system_load) as avg_fore
                FROM system_load
                WHERE EXTRACT(year FROM date_time) = 2007
                GROUP BY 1
            ) slf
            JOIN (
                -- find rescaling values for all years in system_load
                SELECT
                    load_zone,
                    EXTRACT(year FROM date_time) AS year_hist,
                    MAX(system_load) AS peak_hist,
                    AVG(system_load) AS avg_hist
                FROM system_load
                GROUP BY 1, 2
            ) slh USING (load_zone)
            ORDER by load_zone, year_hist, year_fore;
        UPDATE tsls
            SET scale = (peak_fore - avg_fore) / (peak_hist - avg_hist);
        UPDATE tsls
            SET "offset" = peak_fore - scale * peak_hist;
        DELETE FROM system_load_scale WHERE load_scen_id='flat_2007';
        INSERT INTO system_load_scale SELECT * FROM tsls;
        DROP TABLE tsls;
    """)


    def system_load_exp():
        import scipy.optimize

        # rescale loads using scale * (mw ** exp) model instead of scale * mw + offset

        # TODO: recreate system_load from "data/FERC Load Data/Part 3 Schedule 2 -
        # Planning Area Hourly Demand with headers.csv"

        # get historical loads
        hourly = pd.read_sql(
            sql="""
                SELECT load_zone, date_time, system_load
                FROM system_load
                ORDER BY 1, 2;
            """,
            con=db_engine
        )
        # dates are stored in the database in HST but they come out in UTC, so the years
        # won't split correctly.
        hourly['date_time'] = hourly['date_time'].dt.tz_convert('Pacific/Honolulu')
        hourly['year'] = hourly['date_time'].dt.year

        # forecast peak and energy
        fore = data_frame_from_xlsx(technology_data_file, 'sales_forecast')
        fore = fore.T.set_index(0).T
        fore = fore.rename(columns={'year': 'year_fore'})
        fore['peak_fore'] = fore['underlying forecast (MW)'] + fore['energy efficiency (MW)']
        fore['avg_fore'] = (fore['underlying forecast (GWh)'] + fore['energy efficiency (GWh)'])/8.76
        fore = fore.reindex(columns=['load_zone', 'year_fore', 'peak_fore', 'avg_fore'])

        # adjust exp until peak/mean has correct ratio, then set scale to get correct mean.
        # TODO: turn the code below into a loop over historical and forecast years and save
        # the results back into the system_load_scale table, which will need an extra exponent
        # column, and revise scenario_data.py to use the exponent before  the scale and offset.
        # note: this has not been completed because the results are nearly indistinguishable
        # from the normal system_load code, which uses a linear transformation. Either way,
        # 2007-08 loads can only be transformed into HECO's 2045 forecast by reducing peaks
        # while retaining the baseload, so the mean is nearly the same but the peak is much lower.
        year_hist = 2007
        year_fore = 2045

        hist_load = hourly.loc[hourly['year']==year_hist, 'system_load'].values
        hist_mean = hist_load.mean()
        hist_peak = hist_load.max()
        fore_mean = fore.loc[fore['year_fore']==year_fore, 'avg_fore'].iloc[0]
        fore_peak = fore.loc[fore['year_fore']==year_fore, 'peak_fore'].iloc[0]
        fore_ratio = fore_peak/fore_mean
        # how close are we to the target ratio?
        mismatch = lambda exp: (hist_peak ** exp) / (hist_load ** exp).mean() - fore_ratio
        exp = scipy.optimize.newton(mismatch, 1.0)
        scale = fore_peak / (hist_peak ** exp)

        lin_scale = (fore_peak - fore_mean) / (hist_peak - hist_mean)
        lin_offset = fore_peak - lin_scale * hist_peak

        test = pd.DataFrame({
            'hist': hist_load,
            'hist_exp': (hist_load ** exp) * scale,
            'hist_lin': hist_load * lin_scale + lin_offset
        })
        # %matplotlib inline
        test.iloc[200*24:208*24, :].plot(ylim=(0,1500))

        print("Not saving load_scen_id {} because system_load_exp has not been completely written.")

def interconnects():
    # also see calculate_interconnect_costs() for code to fill in
    # project.interconnect_id, project.connect_distance_km and project.spur_line_cost_per_mw
    # based on this table
    # note: we could eventually add interconnect-specific connection costs here,
    # to be used instead of generic project interconnection costs; in that case
    # the code in calculate_interconnect_costs() would also need to be updated
    execute("""
        DROP TABLE IF EXISTS interconnect;
        CREATE TABLE interconnect (
            interconnect_id integer PRIMARY KEY NOT NULL,
            county text,
            latitude float,
            longitude float
        );
        -- ALTER TABLE interconnect OWNER TO admin;
        -- At some point interconnect was filled in with the equivalent of the
        -- following command. The original code is missing, but these appear to be
        -- the population-weighted centers of each county.
        INSERT INTO interconnect (interconnect_id, county, latitude, longitude) VALUES
            (1, 'Honolulu', 21.372464, -157.913673),
            (2, 'Hawaii', 19.672837, -155.421895),
            (3, 'Maui', 20.863747, -156.493816),
            (4, 'Kauai', 22.021022, -159.442112),
            (5, 'Kalawao', 21.188495, -156.979972);
    """)

def calculate_interconnect_costs():
    """Choose closest interconnect location to each project, and calculate distance to it.
    Also calculate spur_line_cost_per_mw based on distance and generic connection cost for
    each technology.
    note: this could eventually be updated to use interconnect-specific costs, where
    provided, instead of generic project interconnect costs; in that case, code that
    creates the interconnect table in import_data.py would need to be updated.
    """
    execute("""
        WITH distances as (
            select p.project_id, i.interconnect_id,
                -- haversine distance formula, radius of earth = 6371 km
                2 *  6371 * sqrt(
                    pow(sin(radians((i.latitude - p.latitude)/2)), 2)
                    + cos(radians(p.latitude)) * cos(radians(i.latitude))
                        * pow(sin(radians((i.longitude - p.longitude)/2)), 2))
                as distance
                from project p, interconnect i
                where p.latitude is not null and p.longitude is not null
        ), closest as (
            select project_id, min(distance) as distance
                from distances group by 1
        ), neighbor as (
            select c.project_id, d.interconnect_id, c.distance
                from closest c join distances d using (project_id, distance)
            -- note, this may return multiple interconnects with the same distance
            -- but that is rare, and one will be chosen arbitrarily in the update query
        )
        update project p
            set interconnect_id = n.interconnect_id,
                connect_distance_km = n.distance
            from neighbor n
            where n.project_id = p.project_id;
    """)
    # The following query will set spur line cost to 0 if latitude or longitude are missing
    # (used for projects that are assumed to be near the load center already)
    execute("""
        update project p
            set spur_line_cost_per_mw =
                %(spur_line_cost_per_mw_km)s * coalesce(connect_distance_km, 0)
            from generator_info g
            where g.technology=p.technology;
    """, dict(spur_line_cost_per_mw_km=get_named_cell_from_xlsx(technology_data_file, 'spur_line_cost_per_mw_km')))


def load_zones():
    execute("""
        DROP TABLE IF EXISTS load_zone;
        CREATE TABLE load_zone (
            load_zone text NOT NULL,
            lat double precision,
            lon double precision,
            ord integer
        );
        COMMENT ON TABLE load_zone IS 'lat and lon show the center of each zone.
        ord shows the order to list the zones in when making reports';
        INSERT INTO load_zone (load_zone, lat, lon, ord)
            VALUES
                ('Oahu', 21.372464, -157.913673, 1),
                ('Hawaii', 19.672837, -155.421895, 2),
                ('Maui', 20.863747, -156.493816, 3),
                ('Kauai', 22.021022, -159.442112, 4),
                ('Kalawao', 21.188495, -156.979972, 5)
        ;
        ALTER TABLE load_zone
            ADD CONSTRAINT load_zone_pkey1 PRIMARY KEY (load_zone);
    """)


# if __name__ == "__main__":
#     main()
