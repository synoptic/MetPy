#!/usr/bin/env ipython3

# Dependencies
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import cartopy.feature as cfeature
import pandas as pd
import datetime
from zoneinfo import ZoneInfo

from metpy.remote.SynopticData import SynopticData
from metpy.plots import USCOUNTIES
import metpy.calc as mpcalc
#from metpy.cbook import get_test_data
from metpy.plots import add_metpy_logo, StationPlot
from metpy.units import units

# Make data request
service = 'Latest'
station = {'state': 'MT',
           'vars': ['air_temp', 'wind_speed', 'wind_direction']}
time = {'within': 120}
opt_params = {'obtimezone': 'local',
              'units': 'temp|F'}
latest = SynopticData('demotoken', service, station, time, opt_params)
mt_data, mt_units, mt_meta  = latest.request_data()

# Remove any stations lacking temperature, wind_speed, or wind_direction
rows_with_nan = [index for index, row in mt_data.iterrows() if row.isnull().any()]
stid_nan = [i[0] for i in rows_with_nan]
stid_nan = list(set(stid_nan))
mt_data = mt_data.drop(rows_with_nan)
mt_meta = mt_meta.drop(stid_nan)


# Reduce point density for visualization
proj = ccrs.LambertConformal(central_longitude=-109)
point_locs = proj.transform_points(ccrs.PlateCarree(), mt_meta['lon'].values, mt_meta['lat'].values)
reduction = mpcalc.reduce_point_density(point_locs, 50 * units.km)
mt_data = mt_data[reduction]
mt_meta = mt_meta[reduction]

# Read in data & assign units to the data to be plotted
temperature = mt_data['air_temp_value_1'].values * units[mt_units['air_temp_value_1']]
wind_speed = mt_data['wind_speed_value_1'].values * units[mt_units['wind_speed_value_1']]
wind_direction = mt_data['wind_direction_value_1'].values * units[mt_units['wind_direction_value_1']]
latitude = mt_meta['lat'].values
longitude = mt_meta['lon'].values
station_id = mt_meta.index.values

# Take cardinal direction and convert to degrees, then convert to components
u, v = mpcalc.wind_components(wind_speed.to('knots'), wind_direction)

# Create the figure and an axes set to the projection.
fig = plt.figure(figsize=(20, 8))
add_metpy_logo(fig, 70, 30, size='large')
ax = fig.add_subplot(1, 1, 1, projection=proj)

# Add some various map elements to the plot to make it recognizable.
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.STATES.with_scale('50m'))

# Set plot bounds
ax.set_extent((-116.3, -103.8, 44, 49.2))

stationplot = StationPlot(ax, longitude, latitude, clip_on=True,
                          transform=ccrs.PlateCarree(), fontsize=8)

# Plot the temperature
stationplot.plot_parameter('NW', temperature, color='red')

# Add wind barbs
stationplot.plot_barb(u, v)

# Also plot the actual text of the station id. Instead of cardinal directions,
# plot further out by specifying a location of 2 increments in x and -1 in y.
stationplot.plot_text((2, -1), station_id)

# Add title and display figure
now = datetime.datetime.now(ZoneInfo('US/Mountain'))
now_string = datetime.datetime.strftime(now, format='%H%M%Z, %d %b, %Y')
plt.title('Montana Temperature: Data Provided by Synoptic Data', fontsize=12, loc='left')
plt.title('Within {} minutes of {}'.format(time['within'], now_string), fontsize=12, loc='right')
plt.show()
