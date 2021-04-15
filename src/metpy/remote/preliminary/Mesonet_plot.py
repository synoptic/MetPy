"""
Test case for integrating Synoptic API call in to MetPy tools
"""

#%% Dependencies
import build_API_call as buildAPI
import pandas as pd
import json
import urllib.request

# Metpy dependencies: 
# NOTE: The metpy package can be installed from conda: 'conda install -c conda-forge metpy'
# And with pip as 'pip install metpy'

# To install cartopy on Ubuntu, must first install:
# ================================================
# (with pip): geos, numpy, cython, pyshp, six (NOT shapely!)
# Then, must run the following:
# $ sudo apt-get install libproj-dev proj-data proj-bin
# $ sudo apt-get install libgeos-dev
# shapely needs to be built from source to link to geos. If it is already
# installed, uninstall it with: 'pip3 uninstall shapely'
# $ pip3 install shapely --no-binary shapely
# Then, 'pip install cartopy' is successful
# https://scitools.org.uk/cartopy/docs/latest/installing.html
# https://stackoverflow.com/questions/53697814/using-pip-install-to-install-cartopy-but-missing-proj-version-at-least-4-9-0
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt

import metpy.calc as mpcalc
from metpy.plots import add_metpy_logo, StationPlot
from metpy.units import units


#%% Pull data from Synoptic API
# -----------------------------
# Construct time series url string
stn         = {'selector': 'state',
               'value': 'mt'}
# stn         = {'selector': 'radius',
#                'value' : ['44.5','-111.','120']}
# vars    = {'vars': ['air_temp', 'wind_speed', 'pressure', 'dew_point_temperature', 'wind_direction'],
#            'units': ['temp|C', 'pres|mb']}
vars = {'vars': ['air_temp', 'wind_speed', 'pressure', 'dew_point_temperature', 'wind_direction'],
        'units': ['temp|C']}
latest = {'within': '60'}

url = buildAPI.api_str_latest(stn, vars, latest=latest)

# Grab data
response = urllib.request.urlopen(url)
data = json.loads(response.read())


#%% Parse data in to Pandas dataframe
# -----------------------------------
# Flatten data to pandas DATAFRAME -- this returns far more data columns than is necessary and will
# need to be culled
df_json = pd.json_normalize(data['STATION'])

# Identify all sensor variables reported by all stations -- this includes data appended with
# ..._1, ..._2, ..._1d (for derived?), etc.
# NOTE: This block is used only to gather information about variables for now
svar = []
n = 0
loc = 0
for station in data['STATION']:        
    for key in station['SENSOR_VARIABLES'].keys():
        d = station['SENSOR_VARIABLES'][key]
        for key in d:
            if key not in svar:
                svar.append(key)

# Variable names -- needed to cull the flattened table
# NOTE: For now I'm using measured temperature, wind speed, and wind direction.
#       Derived pressure and dew point temp are being used b/c measured quantities for these 
#       variables are rare.
msr_vars=vars['vars']
var_names_long = []
for s in msr_vars:
    if s == 'dew_point_temperature' or s == 'pressure':
        var_names_long.append('OBSERVATIONS.' + s + '_value_1d.value')
    else:
        var_names_long.append('OBSERVATIONS.' + s + '_value_1.value')

fields = ['STID','LONGITUDE','LATITUDE','ELEVATION'] + var_names_long 

# Trim dataframe to fields only
df = df_json[fields]

# Rename columns by short name
df.rename(columns = dict(zip(var_names_long, msr_vars)), inplace=True)

# Remove NaNs
df = df.dropna(how='any', subset=msr_vars)


#%% Deal with approximate time and units issues 
# Nominal time of data --  note this is not exact since each station may return a different
# time steamp
t_col = 'OBSERVATIONS.' + msr_vars[0] + '_value_1.date_time'
time = df_json[t_col][0]

# Deal with Units
df_units = data['UNITS']
# Replace non-conforming units names with conforming names for MetPy
#NOTE: This will be a decision to make w/ integration -- change Synoptic unit reporting
# upstream?
unit_keys = list(df_units.keys())
unit_vals = list(df_units.values())
unit_vals = [item.replace('Celsius', 'degC') for item in unit_vals]
unit_vals = [item.replace('Pascals', 'pascals') for item in unit_vals]

df_units = dict(zip(unit_keys, unit_vals))

#%% Practice generating MetPy plot
# More Data wrangling
# Lat, lon extents for different test cases
latlon_meta = {'mt':{'lon_cen': -110, 'extent':(-117, -103, 44., 49.5)},
               'victor':{'lon_cen': -111, 'extent': (-115,-107,42., 47.)}}

# Reduce station density for plotting purposes
proj = ccrs.LambertConformal(central_longitude=latlon_meta['victor']['lon_cen'])
point_locs = proj.transform_points(ccrs.PlateCarree(), df['LONGITUDE'].values, df['LATITUDE'].values)
df = df[mpcalc.reduce_point_density(point_locs, 50 * units.km)]

# Read in the data and assign units as defined by the Mesonet
temperature = units.Quantity(df['air_temp'].values, df_units['air_temp'])
dewpoint = units.Quantity(df['dew_point_temperature'].values, df_units['dew_point_temperature'])
pressure = units.Quantity(df['pressure'].values, df_units['pressure'])
wind_speed = units.Quantity(df['wind_speed'].values, df_units['wind_speed'])
wind_direction = df['wind_direction'].values
latitude = df['LATITUDE'].astype(float)
longitude = df['LONGITUDE'].astype(float)
station_id = df['STID']

# Take cardinal direction and convert to degrees, then convert to components
u, v = mpcalc.wind_components(wind_speed.to('knots'), wind_direction)

# PLOT
#%matplotlib auto #Uncomment this when running interactive window for plotting in separate window
# Create the figure and an axes set to the projection.
fig = plt.figure(figsize=(20, 8))
add_metpy_logo(fig, 70, 30, size='large')
ax = fig.add_subplot(1, 1, 1, projection=proj)

# Add some various map elements to the plot to make it recognizable.
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.STATES.with_scale('50m'))

# Set plot bounds
ax.set_extent(latlon_meta['victor']['extent'])

stationplot = StationPlot(ax, longitude.values, latitude.values, clip_on=True,
                          transform=ccrs.PlateCarree(), fontsize=8)

# Plot the temperature and dew point to the upper and lower left, respectively, of
# the center point. Each one uses a different color.
stationplot.plot_parameter('NW', temperature, color='red')
stationplot.plot_parameter('SW', dewpoint, color='darkgreen')

# A more complex example uses a custom formatter to control how the sea-level pressure
# values are plotted. This uses the standard trailing 3-digits of the pressure value
# in tenths of millibars.
stationplot.plot_parameter('NE', pressure.m, formatter=lambda v: format(10 * v, '.0f')[-3:])

# Add wind barbs
stationplot.plot_barb(u, v)

# Also plot the actual text of the station id. Instead of cardinal directions,
# plot further out by specifying a location of 2 increments in x and -1 in y.
stationplot.plot_text((2, -1), station_id)

# Add title and display figure
plt.title('Victor Area Mesonet Observations', fontsize=16, loc='left')
plt.title('Time:' + time , fontsize=16, loc='right')
plt.show()