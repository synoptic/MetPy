# Copyright (c) 2018 MetPy Developers.
# Distributed under the terms of the BSD 3-Clause License.
# SPDX-License-Identifier: BSD-3-Clause
"""
===============================================
Merging Synoptic Data with MetPy Plotting Tools
===============================================

An example to illustrate the integration of a Synoptic Data data request with
MetPy plotting tools. Request and plot recent temperature and wind conditions over
the state of Utah.
"""

######################################################################
# Dependencies
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import cartopy.feature as cfeature
import pandas as pd
import datetime
from zoneinfo import ZoneInfo

from metpy.remote import SynopticData
from metpy.plots import USCOUNTIES
import metpy.calc as mpcalc
#from metpy.cbook import get_test_data
from metpy.plots import add_metpy_logo, StationPlot
from metpy.units import units

######################################################################
# Make data request to Synoptic Data Latest API. Focus the request on NWS (1),
# RAWS (2), and UTDOT (4) networks
service = 'Latest'
station = {'state': 'UT',
           'network': [1, 2, 4],
           'vars': ['air_temp', 'wind_speed', 'wind_direction']}
time = {'within': 120}
opt_params = {'obtimezone': 'local',
              'units': 'temp|F'}
latest = SynopticData('demotoken', service, station, time, opt_params)
ut_data, ut_units, ut_meta  = latest.request_data()

######################################################################
# Remove any stations lacking temperature, wind_speed, or wind_direction
rows_with_nan = [index for index, row in ut_data.iterrows() if row.isnull().any()]
stid_nan = [i[0] for i in rows_with_nan]
stid_nan = list(set(stid_nan))
ut_data = ut_data.drop(rows_with_nan)
ut_meta = ut_meta.drop(stid_nan)

######################################################################
# Reduce point density for visualization
proj = ccrs.LambertConformal(central_longitude=-111.8)
point_locs = proj.transform_points(ccrs.PlateCarree(), ut_meta['lon'].values, ut_meta['lat'].values)
reduction = mpcalc.reduce_point_density(point_locs, 30 * units.km)
ut_data = ut_data[reduction]
ut_meta = ut_meta[reduction]

######################################################################
# Read in data & assign units to the data to be plotted
temperature = ut_data['air_temp_value_1'].values * units[ut_units['air_temp_value_1']]
wind_speed = ut_data['wind_speed_value_1'].values * units[ut_units['wind_speed_value_1']]
wind_direction = ut_data['wind_direction_value_1'].values * units[ut_units['wind_direction_value_1']]
latitude = ut_meta['lat'].values
longitude = ut_meta['lon'].values
station_id = ut_meta.index.values

######################################################################
# Convert wind direction/speed to components, in units of knots
u, v = mpcalc.wind_components(wind_speed.to('knots'), wind_direction)

######################################################################
# Timing info for the plot title
now = datetime.datetime.now(ZoneInfo('US/Mountain'))
now_string = datetime.datetime.strftime(now, format='%H%M%Z, %d %b, %Y')

######################################################################
# The remainder of this example follows closely other mesonet plotting examples.
# Create the figure and an axes set to the projection.
fig = plt.figure(figsize=(12, 8))
add_metpy_logo(fig, 70, 30, size='large')
ax = fig.add_subplot(1, 1, 1, projection=proj)
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.STATES.with_scale('50m'))
ax.set_extent((-114.5, -108.8, 36.7, 42.3))
stationplot = StationPlot(ax, longitude, latitude, clip_on=True,
                          transform=ccrs.PlateCarree(), fontsize=6)
stationplot.plot_parameter('NW', temperature, color='red')
stationplot.plot_barb(u, v)
stationplot.plot_text((2, -1), station_id)
plt.title('Utah Temperature & Wind: Data Provided by Synoptic Data', fontsize=12, loc='left')
plt.text(0, -0.02, 'Within {} minutes of {}'.format(time['within'], now_string),
         fontsize=12, transform=ax.transAxes, horizontalalignment='left',
         verticalalignment='top')
plt.show()
