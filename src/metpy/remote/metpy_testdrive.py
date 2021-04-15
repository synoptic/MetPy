from SynopticData import TimeSeries
import pandas as pd

#NOTE: For these 3 stns, returning all variables yields 42 columns. And many are
# strings that are useless for MetPy. Do we limit returns to those are strictly 
# useful for MetPy? Set default to air temperature only?
#station = {'stid':'COOPMSOM8,KMSO,C4884'}

# Test 0: multiple stations & variables
# Inputs
station = {'stid':'K4S2,KMSO', 
           'vars':'air_temp,pressure,precip_accum'}
time = {'start': 202104010000, \
        'end': 202104200000}

# Instantiate timeseries class
ts = TimeSeries('demotoken', station, time)
ts.estimate_usage()
ts.request_data()

# Final dataframes
df = ts.data_df
meta_df = ts.meta_df

# Various querying tests
# Variables
vars = df.columns.values
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



