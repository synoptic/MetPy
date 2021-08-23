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
opt_params = {'qc': 'on'}
# Instantiate class
timeseries = SynopticData('demotoken', service, station, time, opt_params)

# Make data request
stn_data, stn_units, stn_meta, qc = timeseries.request_data()

# Report all temperature and attach units
column = 'air_temp_set_1'
all_temp = stn_data[column].values * ureg(stn_units[column])
# Report kmso temperature and attach units
kmso_temp = stn_data.loc['KMSO',column].values * ureg(stn_units[column])

# Slice air temp at KMSO by a specific time range
t0 = pd.to_datetime("08/15/21 12:00").tz_localize('UTC')
t1 = pd.to_datetime("08/15/21 21:00").tz_localize('UTC')
stn_data.loc['KMSO', 'air_temp_set_1'][t0:t1]


# ==============================================================================
# Test 1: Change stations and units
# ==============================================================================
station = {'state': 'mt',
           'vars': ['air_temp', 'wind_gust']}
time = {'recent':60}
opt_params = {'qc': 'on',
              'units': ['speed|mph', 'temp|F']}
# No need to re-instantiate the class!
timeseries.station = station
timeseries.time = time
timeseries.opt_params = opt_params
mt_data, mt_units, mt_meta, mt_qc = timeseries.request_data()
# Report all wind_gust
column = 'wind_gust_set_1'
all_gust = mt_data.loc[:,column].values * ureg(mt_units[column])


# ==============================================================================
# Test 2: Mimic the call on the QC page on the API to get a query with flagged
# data
# ==============================================================================
station = {'radius': ['KHOU','30'],
           'vars': ['air_temp', 'wind_speed']}
time = {'start': 201906170000,
        'end': 201906190000}
opt_params = {'qc':'on'}
timeseries.station = station
timeseries.time = time
timeseries.opt_params = opt_params
khou_data, khou_units, khou_meta, khou_qc = timeseries.request_data()


# ==============================================================================
# Test 3: Change time format, new network, try a new variables w difft units
# ==============================================================================
station = {'county':'missoula',
           'state': 'mt',
           'vars': ['solar_radiation', 'wind_gust']}
time = {'recent':60}
opt_params = {'obtimezone': 'local',
              'units': ['speed|mph']}
timeseries.station = station
timeseries.time = time
timeseries.opt_params = opt_params
mso_data, mso_units, mso_meta = timeseries.request_data()


# ==============================================================================
# Test 4: MT air quality!!!
# ==============================================================================
# integer and string list elements are handled
station = {'radius': ['KMSO', 60],
           'vars': 'PM_25_concentration'}
time = {'recent': 120}
opt_params = {'obtimezone': 'local'}
timeseries.station = station
timeseries.time = time
timeseries.opt_params = opt_params
aq_data, aq_units, aq_meta = timeseries.request_data()
# Check units
column = 'PM_25_concentration_set_1'
pm25 = aq_data.loc[:, column].values * ureg(aq_units[column])

# ==============================================================================
# Test 5: MT mesonet
# ==============================================================================
# integer and string list elements are handled
station = {'stid': 'MTM06'}
time = {'recent': 120}
opt_params = {'obtimezone': 'local'}
timeseries = SynopticData('demotoken', 'TimeSeries', station, time, opt_params)
mtm_data, mtm_units, mtm_meta = timeseries.request_data()

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
mt_data, mt_units, mt_meta = latest.request_data()

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
pdx_data, pdx_units, pdx_meta = nearest.request_data()

# ==============================================================================
# Test 7: Precip test
# ==============================================================================
service = 'Precip'
station = {'stid': ['KMSO','KSLC']}
time = {'start': 202108200000,
        'end': 202108230000}
opt_params = {'obtimezone': 'local',
              'pmode': 'intervals',
              'interval': 4}
precip = SynopticData('yo', service, station, time, opt_params)
precip_data, precip_units, precip_meta = precip.request_data()



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

