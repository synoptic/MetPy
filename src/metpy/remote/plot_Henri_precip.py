#!/usr/bin/env ipython3
"""
Plot weekend precip totals during tropical storm Henri
"""

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
service = 'Precip'
station = {'bbox': [-75.3,40.,-69.5,43.],
           'spacing': 600,
           'networkimportance': ['1','2']}
time = {'start': 202108210000,
        'end': 202108240000}
opt_params = {'obtimezone': 'local'}
precip = SynopticData('yo', service, station, time, opt_params)
henri_data, henri_units, henri_meta  = precip.request_data()

# Remove any stations lacking data
rows_with_nan = [index for index, row in henri_data.iterrows() if row.isnull().any()]
stid_nan = [i[0] for i in rows_with_nan]
stid_nan = list(set(stid_nan))
henri_data = henri_data.drop(rows_with_nan)
henri_meta = henri_meta.drop(stid_nan)


# Reduce point density for visualization
proj = ccrs.LambertConformal(central_longitude=-71)
point_locs = proj.transform_points(ccrs.PlateCarree(), henri_meta['lon'].values, henri_meta['lat'].values)
reduction = mpcalc.reduce_point_density(point_locs, 30 * units.km)
henri_data = henri_data[reduction]
henri_meta = henri_meta[reduction]

# Read in data & assign units to the data to be plotted
precipitation = henri_data['precipitation'].values * units[henri_units['precipitation']]
precip_in = precipitation.to('inch')
latitude = henri_meta['lat'].values
longitude = henri_meta['lon'].values
station_id = henri_meta.index.values


# Create the figure and an axes set to the projection.
fig = plt.figure(figsize=(20, 8))
add_metpy_logo(fig, 70, 30, size='large')
ax = fig.add_subplot(1, 1, 1, projection=proj)

# Add some various map elements to the plot to make it recognizable.
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.STATES.with_scale('50m'))

# Set plot bounds
ax.set_extent((-75.5, -69.5, 40., 43.))

stationplot = StationPlot(ax, longitude, latitude, clip_on=True,
                          transform=ccrs.PlateCarree(), fontsize=8)

ax.plot(longitude, latitude, 'bo', transform=ccrs.PlateCarree())
# Plot the temperature
stationplot.plot_parameter('NW', precip_in, color='blue', formatter='0.1f')

# Also plot the actual text of the station id. Instead of cardinal directions,
# plot further out by specifying a location of 2 increments in x and -1 in y.
stationplot.plot_text((2, -1), station_id)

# Add title and display figure
from_date = datetime.datetime.strptime(str(time['start']), '%Y%m%d%H%M')
to_date = datetime.datetime.strptime(str(time['end']), '%Y%m%d%H%M')
from_string = datetime.datetime.strftime(from_date, format='%m/%d/%Y')
to_string = datetime.datetime.strftime(to_date, format='%m/%d/%Y')
plt.title('Precip totals (inches): Data Provided by Synoptic Data', fontsize=12, loc='left')
plt.title(f'{from_string} - {to_string}', fontsize=12, loc='right')
plt.show()
