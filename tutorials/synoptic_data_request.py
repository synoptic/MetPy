# Copyright (c) 2018 MetPy Developers.
# Distributed under the terms of the BSD 3-Clause License.
# SPDX-License-Identifier: BSD-3-Clause
"""
==============================================================
Incorporating Real-Time Data From Synoptic Data's API Services
==============================================================

`Synoptic Data <www.synopticdata.com>`_ is a Public Benefit Corporation dedicated to
aggregating and disseminating station-based Earth data. Born out of the University
of Utah's `MesoWest program <https://mesowest.utah.edu/>`_, and now serving as one
of the National Mesonet Program's lead subcontractors, Synoptic provides access to
real-time and historical data from >100,000 stations globally through it's
`Mesonet API services <https://synopticdata.com/mesonet-api>`_. Supporting educational and
research initiatives is central to the company's mission.

MetPy includes capabilities to leverage station-based meteorological data by seamlessly
integrating with Synoptic's API data services. Tailor your query to your specifications,
and the package will send a data request to Synoptic's API, parse the returned data, and
treat units in a pint-compatible format to facilitate incorporating Synoptic's data aggregation services
in your MetPy workflow.

In this tutorial we'll provide an introduction to requesting data and working with
the returned output.

Synoptic Data's API: A Brief Background
---------------------------------------

Synoptic Data offers four primary API services for accessing data:

1. `Time Series <https://developers.synopticdata.com/mesonet/v2/stations/timeseries/>`_

2. `Nearest Time <https://developers.synopticdata.com/mesonet/v2/stations/nearesttime/>`_

3. `Latest <https://developers.synopticdata.com/mesonet/v2/stations/latest/>`_

4. `Precip <https://developers.synopticdata.com/mesonet/v2/stations/precipitation/>`_

To get started, you need a `user account and token <https://developers.synopticdata.com/signup/>`_.
Academic and non-commercial usage is free, and includes generous limits on
`monthly API usage <https://synopticdata.com/pricing>`_.

And of course, you'll need an idea of the data you're interested in so you can specify
the parameters for your request! Having trouble finding your station? Browse
`Synoptic's Data Explorer <https://explore.synopticdata.com>`_ to get a sense of the data
they have to offer.

Ok, let's get started!

"""

#########################################################################
# First... the imports
# ----------------------
from metpy.remote import SynopticData
from metpy.units import units
import pandas as pd

#########################################################################
# Latest Service
# --------------
# The parameters to be specified in an API request are detailed in the links above
# for each service. Many parameters are identical across services. In this SynopticData
# package, we broadly group parameters into dictionaries relating to 1) station/variable,
# 2) time, and 3) optional arguments. *Parameter:selector* request arguments are
# specified as *key:value* dictionary pairs.
#
# Let's begin with a *Latest* service example, and query the most recent (within 2 hrs)
# temperature, wind speed, and wind direction over the state of Utah. Further, let's
# limit our query to stations in the NWS and Utah DOT networks.

# Begin by defining the service, station and time dictionaries, and user token.
service = 'Latest'
station = {'state': 'UT',
           'network': ['1', '4'],
           'vars': ['air_temp', 'wind_speed', 'wind_direction']}
time = {'within': 120}
token = 'demotoken'

#########################################################################
# Instantiate the SynopticData class with these variables and your Synoptic Data token:
latest = SynopticData(token, service, station, time)

#########################################################################
# And make the request (as part of the request process, the request url sent
# to Synoptic's API is printed):
ut_data, ut_units, ut_meta = latest.request_data()

#########################################################################
# *request_data* formats the API url string, sends the request, parses
# the data return, and returns data, units, and metadata objects. The data object
# is formatted as a Pandas dataframe, multi-indexed by station id and datetime.
# Columns are data variables, in Synoptic Data parlance:
ut_data

#########################################################################
# The units object is a dictionary specifying the units of each variable (using
# Synoptic unit strings):
ut_units

#########################################################################
# And the metadata is a Pandas dataframe, indexed by stid:
ut_meta

#########################################################################
# Key strings in the units dictionary match the dataframe columns, simplifying the process
# of attaching units to the data. This can be accomplished in a convention similar to
# elsewhere on the MetPy site:
column = 'air_temp_value_1'
sample_temperature = ut_data[column].values[0:5] * units(ut_units[column])
print(sample_temperature)

#########################################################################
# NearestTime Service
# -------------------
# The Nearest Time service permits querying data nearest to a specified date/time.
# In this example, we'll query air temperature from Oregon's Multnomah County (encompassing
# Portland) during the June heat wave.
#
# The set-up and request are very similar to Latest:
service = 'NearestTime'
station = {'county': 'multnomah',
           'state': 'OR',
           'vars': 'air_temp'}
time = {'attime': 202106280000,
        'within': 120}

#########################################################################
# The requested time parameters must be in UTC, but data can be returned in the
# local time zone. We can also specify units in the API request. Set these
# optional parameters:
opt_params = {'obtimezone': 'local',
              'units': 'temp|F'}

#########################################################################
# Once again, instantiate the class and make the request:
nearest = SynopticData(token, service, station, time, opt_params)
portland_data, portland_units, portland_meta = nearest.request_data()

#########################################################################
# A quick glimpse of the return shows the expected... record high temps:
portland_data.sort_values('air_temp_value_1', ascending=False).iloc[0:10]

#########################################################################
# TimeSeries Service
# ------------------
# The Time Series service motivates the usage of Panda's MultiIndex data structure.
# Let's illustrate this by querying data from the SLC airport and William Brown Building
# (home of Univ. Utah's Atmospheric Science Dept).
service = 'TimeSeries'
station = {'stid': ['KSLC', 'WBB'],
           'vars': ['air_temp','wind_speed']}
time = {'start': 202108030000,
        'end': 202108040000}

timeseries = SynopticData(token, service, station, time)
stn_data, stn_units, stn_meta = timeseries.request_data()

#########################################################################
# The returned data object contains timeseries data for each site:
stn_data

#########################################################################
# The data object structure permits flexible slicing. Stations & data variables
# can be specified:
idx = pd.IndexSlice
stn_data.loc[idx['KSLC',:], 'air_temp_set_1']

#########################################################################
# Subsetting a specific time range can also be achieved by acting on the resulting
# dataframe:
t0 = pd.to_datetime("08/03/21 12:00").tz_localize('UTC')
t1 = pd.to_datetime("08/03/21 18:00").tz_localize('UTC')
stn_data.loc[idx['KSLC',t0:t1], 'air_temp_set_1']

#########################################################################
# Slicing on the second level (time) can also be performed across all stations:
stn_data.loc[idx[:, t0:t1], 'air_temp_set_1']

#########################################################################
# Precip service
# --------------
# Requests to the precipitation service follow the same general procedure as those above, but
# some differences exist to deal with the fact that precipitation must be measured over an interval of time.
# Users should define *pmode* according to the precip output desired (see the
# API docs for more info). The returned dataframe indices also slightly differ, and
# identify the interval (*first_report*, *last_report*) over which the results are measured/computed. Here's an
# example returning the precipitation totals from stations in a 30 mile radius
# of the SeaTac airport over a 3 day period in January, 2020.
service = 'Precip'
station = {'radius': ['KSEA',30]}
time = {'start': 202001200000,
        'end': 202001230000}
opt_params = {'pmode': 'totals',
              'obtimezone': 'local'}
precip = SynopticData('demotoken', service, station, time, opt_params)
precip_data, precip_units, precip_meta = precip.request_data()
precip_data.sort_values('precipitation', ascending=False).iloc[0:10]

#########################################################################
# *count* is the measurement counts during time interval, and *precipitation* is
# the precipitation total for each station in units of millimeters (Synoptic default):
precip_units

#########################################################################
# Interval- or accumulation-based precipitation over user-specified time windows can
# be requested using *pmode=intervals* and *pmode=last*.
opt_params = {'pmode': 'last',
              'accum_hours': [0,24,48,72],
              'obtimezone': 'local'}

#########################################################################
# Parameter dictionaries are attached to the class object to facilitate updating
precip.opt_params = opt_params
precip_data, precip_units, precip_meta = precip.request_data()

#########################################################################
# In these instances, the resulting dataframe contains an additional index column
# to facilitate slicing, similar to the TimeSeries example above:
precip_data

#########################################################################
# 24-hour precipitation totals ending at *time['end']* for each station:
precip_data.loc[idx[:,24,:,:]]
