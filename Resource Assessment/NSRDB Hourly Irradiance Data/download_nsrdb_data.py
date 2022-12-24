"""
This code downloads NSRDB data via the NREL API. It is based on documentation at
https://developer.nrel.gov/docs/solar/nsrdb/guide/ and
https://developer.nrel.gov/docs/solar/nsrdb/python-examples/

To use this code, first copy api_data_sample.py to api_sample.py and fill in
the required information. Then run this script. It will take about 10 minutes
to run.
"""

import urllib, urllib.request, os, time
import numpy as np

from api_data import api_key, user_email, user_name, reason_for_use, user_affiliation

def make_nsrdb_url(lat, lon, year):
    # boilerplate from https://nsrdb.nrel.gov/api-instructions
    # (also see  for newer version)

    # Set the attributes to extract (e.g., dhi, ghi, etc.), separated by commas.
    # attributes = 'ghi,dhi,dni,wind_speed_10m_nwp,surface_air_temperature_nwp,solar_zenith_angle'

    # Please join our mailing list so we can keep you up-to-date on new developments.
    mailing_list = 'false'


    input_data = {
        'attributes': 'ghi,dhi,dni,wind_speed,air_temperature,solar_zenith_angle',
        # time interval in minutes, i.e., '30' is half hour intervals. Valid intervals are 30 & 60.
        'interval': '60',
        # Specify Coordinated Universal Time (UTC), 'true' will use UTC, 'false'
        # will use the local time zone of the data. NOTE: In order to use the
        # NSRDB data in SAM, you must specify UTC as 'false'. SAM requires the
        # data to be in the local time zone.
        'utc': 'false',
        # Get data for center of hour instead of start?
        'half_hour': 'true',
        # Include leap day if this is a leap year ('true' or 'false')
        'leap_day': 'true',
        'api_key': api_key,
        'email': user_email,
        'full_name': urllib.parse.quote_plus(user_name),
        'affiliaton': urllib.parse.quote_plus(user_affiliation),
        'mailing_list': 'false',
        'reason': urllib.parse.quote_plus(reason_for_use),
        'wkt': 'POINT({} {})'.format(lon, lat),
        'names': year,
    }
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-download.csv?"
    url = BASE_URL + urllib.parse.urlencode(input_data, True)
    return url

last_request = time.time() - 1
for year in [2007, 2008]:
    outdir = 'nsrdb oahu {}'.format(year)
    os.makedirs(outdir, exist_ok=True)
    for lon in np.arange(-158.3, -157.66+.04, 0.04):
        for lat in np.arange(21.25, 21.73+.04, 0.04):
            outfile = os.path.join(outdir, 'nsrdb_{:.3f}_{:.3f}_{}.csv'.format(lat, lon, year))
            if os.path.exists(outfile) and os.path.getsize(outfile) > 200:
                print("skipping {}, already downloaded".format(outfile))
            else:
                url = make_nsrdb_url(lat, lon, year)
                # print("downloading from {}".format(url))
                # wait until at least one second after last request because
                # NREL only allows one request per second.
                time.sleep(max(last_request + 1 - time.time(), 0))
                last_request = time.time()
                urllib.request.urlretrieve(url, outfile)
                print("saved {}".format(outfile))

