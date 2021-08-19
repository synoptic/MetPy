from metpy.remote.SynopticData import SynopticData
import pandas as pd
from pint import UnitRegistry

ureg = UnitRegistry()


# ==============================================================================
# Test 0: multiple stations & variables
# ==============================================================================
# Inputs
service = 'TimeSeries'
station = {'stid': ['KMSO', 'KSLC'],
           'vars': ['air_temp', 'wind_speed']}
time = {'start': 202108150000,
        'end': 202108160000}

# Instantiate class
timeseries = SynopticData('demotoken', service, station, time)

# Make data request
stn_data, stn_meta, stn_units = timeseries.request_data()

# Report all temperature and attach units
column = 'air_temp_set_1'
all_temp = stn_data[column].values * ureg(stn_units[column])
# Report kmso temperature and attach units
kmso_temp = stn_data.loc['KMSO',column].values * ureg(stn_units[column])

# Slice air temp at KMSO by a specific time range
t0 = pd.to_datetime("08/15/21 12:00").tz_localize('UTC')
t1 = pd.to_datetime("08/15/21 21:00").tz_localize('UTC')
data_df.loc['KMSO', 'air_temp_set_1'][t0:t1]


# ==============================================================================
# Test 1: Change stations and units
# ==============================================================================
station = {'state': 'mt',
           'vars': ['air_temp', 'wind_gust']}
time = {'recent':60}
opt_params = {'qc': 'on',
              'units': ['speed|mph', 'temp|F']}
# No need to re-instantiate the class!
ts.station = station
ts.time = time
ts.opt_params = opt_params
data_df, meta_df, qc_df, units = ts.request_data()
# Report all wind_gust
column = 'wind_gust_set_1'
all_gust = data_df.loc[:,column].values * ureg(units[column])


f# ==============================================================================
# Test 2: Mimic the call on the QC page on the API to get a query with flagged
# data
# ==============================================================================
station = {'radius': ['KHOU','30'],
           'vars': ['air_temp', 'wind_speed']}
time = {'start': 201906170000,
        'end': 201906190000}
opt_params = {'qc':'on'}
ts.station = station
ts.time = time
ts.opt_params = opt_params
data_df, meta_df, qc_df, units = ts.request_data()


# ==============================================================================
# Test 3: Change time format, new network, try a new variables w difft units
# ==============================================================================
station = {'county':'missoula',
           'state': 'mt',
           'vars': ['solar_radiation', 'wind_gust']}
time = {'recent':60}
opt_params = {'obtimezone': 'local',
              'units': ['speed|mph']}
ts.station = station
ts.time = time
ts.opt_params = opt_params
data_df, meta_df, units = ts.request_data()


# ==============================================================================
# Test 4: MT air quality!!!
# ==============================================================================
# integer and string list elements are handled
station = {'radius': ['KMSO', 60],
           'vars': 'PM_25_concentration'}
time = {'recent': 120}
opt_params = {'obtimezone': 'local'}
ts.station = station
ts.time = time
ts.opt_params = opt_params
data_df, meta_df, units = ts.request_data()
# Check units
column = 'PM_25_concentration_set_1'
pm25 = data_df.loc[:, column].values * ureg(units[column])

# ==============================================================================
# Test 5: MT mesonet
# ==============================================================================
# integer and string list elements are handled
station = {'stid': 'MTM06'}
time = {'recent': 120}
opt_params = {'obtimezone': 'local'}
ts = TimeSeries('demotoken', station, time, opt_params)
data_df, meta_df, units = ts.request_data()


# ==============================================================================
# Test 5: Latest call
# ==============================================================================
# integer and string list elements are handled
service = 'Latest'
station = {'state': 'MT',
           'vars': ['air_temp']}
time = {'minmax': 7}
opt_params = {'obtimezone': 'local'}
latest = SynopticData('demotoken', service, station, time, opt_params)
mt_data, mt_meta, mt_units = latest.request_data()
# LATEST RETURNS DIFFERENT DATA FORMAT!


# ==============================================================================
# Test 6: Universal class test
# ==============================================================================
service = 'NearestTime'
station = {'county': 'multnomah',
           'state': 'OR',
           'vars': 'air_temp'}
time = {'attime': 202106280000,
        'within': 120}
opt_params = {'obtimezone': 'local',
              'units': 'temp|F'}
nearest = SynopticData('demotoken', service, station, time, opt_params)
pdx_data, pdx_meta, pdx_units = nearest.request_data()




# ---------------------------------
# Optional parameters
# - obtimezone
# - showemptystations
# - units
# - precip
# - hfmetars
# - timeformat
# - qc
# - qc_remove_data
# - qc_flags
# - qc_checks

