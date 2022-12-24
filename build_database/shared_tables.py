from __future__ import absolute_import
from util import execute

queries = {}

# note: some of the project data could go in a separate site table,
# but we keep it all in one place for now for simplicity
# ??? can we add existing projects to this table too? (no reason not to;
# may need to add a flag indicating whether more capacity can be built in
# each project.)
# note: we assume max_capacity indicates the max amount of each technology
# if that is the only thing built at this site; if multiple projects
# are built on the same site, we require sum(Build[site, tech]/max_capacity[site, tech]) <= 1.
# note: we use double precision instead of real to avoid rounding errors when comparing
# max_capacity to proj_existing_builds (which ends up with double precision) and generally
# to maintain consistency throughout the work
queries[("project", "create_table")] = """
    CREATE TABLE IF NOT EXISTS project (
        project_id SERIAL PRIMARY KEY,
        load_zone VARCHAR(20),
        technology VARCHAR(50),
        site VARCHAR(30),
        orientation VARCHAR(5),
        max_capacity DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        interconnect_id INT,
        connect_distance_km DOUBLE PRECISION,
        spur_line_cost_per_mw DOUBLE PRECISION
    );
    -- ALTER TABLE project OWNER TO admin;
"""
queries[("cap_factor", "create_table")] = """
    CREATE TABLE IF NOT EXISTS cap_factor (
        project_id INT NOT NULL,
        date_time TIMESTAMP WITH TIME ZONE,
        cap_factor REAL
    );
    -- ALTER TABLE cap_factor OWNER TO admin;
"""
queries[("generator_info", "create_table")] = """
    CREATE TABLE IF NOT EXISTS generator_info (
        tech_scen_id VARCHAR(30) NOT NULL,
        technology VARCHAR(50),
        min_vintage_year INT,
        unit_size DOUBLE PRECISION,
        substation_cost_per_kw DOUBLE PRECISION,
        variable_o_m DOUBLE PRECISION,
        fuel VARCHAR(20),
        heat_rate DOUBLE PRECISION,
        max_age_years INT,
        forced_outage_rate DOUBLE PRECISION,
        scheduled_outage_rate DOUBLE PRECISION,
        intermittent INT,
        resource_limited INT,
        distributed INT,
        baseload INT,
        must_run INT,
        non_cycling INT,
        cogen INT,
        min_uptime DOUBLE PRECISION,  -- hours
        min_downtime DOUBLE PRECISION,  -- hours
        startup_energy DOUBLE PRECISION,  -- MMBtu for whole plant
        base_year INT,
        gen_storage_efficiency DOUBLE PRECISION,
        gen_storage_energy_to_power_ratio DOUBLE PRECISION,
        gen_storage_max_cycles_per_year DOUBLE PRECISION
    );
    -- ALTER TABLE generator_info OWNER TO admin;
"""

# queries[("cap_factor", "create_indexes")] = """
#     DO $$
#     BEGIN
#         BEGIN
#             ALTER TABLE cap_factor
#                 ADD CONSTRAINT pt PRIMARY KEY (project_id, date_time),
#                 ADD CONSTRAINT tp UNIQUE (date_time, project_id)
#         EXCEPTION
#             WHEN duplicate_object THEN NULL; -- ignore if index exists already
#         END;
#     END $$;
# """

# note: if this reports 'relation "pt" already exists', it probably means an index
# named pt is already attached to an old (renamed) version of cap_factor. That can
# be viewed via "select * from pg_indexes where indexname='pt';" and renamed via
# "alter index pt rename to pt_2018_07_23;"
queries[("cap_factor", "create_indexes")] = """
    ALTER TABLE cap_factor
        ADD CONSTRAINT pt PRIMARY KEY (project_id, date_time),
        ADD CONSTRAINT tp UNIQUE (date_time, project_id);
"""
queries[("cap_factor", "drop_indexes")] = """
    ALTER TABLE cap_factor
        DROP CONSTRAINT IF EXISTS pt,
        DROP CONSTRAINT IF EXISTS tp;
"""

queries[("study_periods", "create_table")] = """
    CREATE TABLE IF NOT EXISTS study_periods (
        time_sample character varying(40) NOT NULL,
        period bigint NOT NULL,
        period_end integer
    );
    ALTER TABLE study_periods
        DROP CONSTRAINT IF EXISTS study_periods_pkey,
        ADD CONSTRAINT study_periods_pkey PRIMARY KEY (time_sample, period);
"""

queries[("study_date", "create_table")] = """
    CREATE TABLE study_date (
        period bigint,
        study_date bigint NOT NULL,
        month_of_year integer,
        date date,
        hours_in_sample double precision,
        time_sample character varying(40) NOT NULL,
        ts_num_tps integer,
        ts_duration_of_tp double precision,
        ts_scale_to_period double precision
    );
    ALTER TABLE study_date
        DROP CONSTRAINT IF EXISTS study_date_pkey,
        ADD CONSTRAINT study_date_pkey PRIMARY KEY (time_sample, study_date);
"""

queries[("study_hour", "create_table")] = """
    CREATE TABLE study_hour (
        study_date bigint NOT NULL,
        study_hour bigint NOT NULL,
        hour_of_day integer,
        date_time timestamp with time zone NOT NULL,
        time_sample character varying(40)
    );
    ALTER TABLE study_hour
        DROP CONSTRAINT IF EXISTS study_hour_pkey,
        ADD CONSTRAINT study_hour_pkey PRIMARY KEY
            (time_sample, study_date, study_hour);
"""


def create_table(table):
    execute(queries[(table, "create_table")])

def create_indexes(table):
    if (table, "create_indexes") in queries:
        execute(queries[(table, "create_indexes")])

def drop_indexes(table):
    if (table, "drop_indexes") in queries:
        execute(queries[(table, "drop_indexes")])
