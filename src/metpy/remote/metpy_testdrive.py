from SynopticData import TimeSeries
import pandas as pd
from pint import UnitRegistry

ureg = UnitRegistry()

# Test 0: multiple stations & variables
# Inputs
station = {'stid': 'KSLC,KMSO',
           'vars': 'air_temp,pressure,precip_accum'}
time = {'start': 202104010000,
        'end': 202104011200}

# Instantiate timeseries class
ts = TimeSeries('demotoken', station, time)
#ts.estimate_usage()

# Make data request
data_df, meta_df, units = ts.request_data()


# Temperature
t = 'air_temp_set_1'
temperature_array = data_df.loc['KMSO', t].values * ureg(units[t])



#NOTE: For these 3 stns, returning all variables yields 42 columns. And many are
# strings that are useless for MetPy. Do we limit returns to those are strictly
# useful for MetPy? Set default to air temperature only?
#station = {'stid':'COOPMSOM8,KMSO,C4884'}












# Final dataframes
df = ts.data_df
meta_df = ts.meta_df

raw_data = ts.data
units = raw_data['UNITS']
data = raw_data['STATION']
obs0 = data[0]
variables = obs0['SENSOR_VARIABLES'].keys()

unit_dic = {}
dtypes = {}
c1 = df.columns[0]
for c1 in df.columns:
    for u_name in units.keys():
        if c1.find(u_name) != -1:
            unit = units[u_name]
    dtypes.update({c1: "pint[{}]".format(unit)})
    unit_dic.update({c1: unit})

variable = 'air_temp_set_1'

# Temperature
temp_arr = df.loc['KMSO', variable].values * ureg(unit_dic[variable])





# Various querying tests
# Variables
variables = df.columns.values
# Query data -- Just one station. 
df.loc['KMSO', 'air_temp_set_1']
# Query data by time -- This is challenging in a strict sense b/c stations
# sample at different intervals 
t = pd.to_datetime("04/01/21 12:00").tz_localize('UTC')
ts.data_df.xs(t, level="dattim")
# Query single variable, single station, for time range 
t1 = pd.to_datetime("04/01/21 21:00").tz_localize('UTC')
ts.data_df.loc['KMSO', 'air_temp_set_1'][t:t1]


# Test 1: Multiple stations using 'recent'
station1 = {'county':'missoula',
           'state': 'MT',
           'vars': 'air_temp'}
time1 = {'recent':120}

ts1 = TimeSeries('demotoken', station1, time1)
ts1.estimate_usage()
ts1.request_data()



