"""
Tools to request data from Synoptic Data's API services and parse/format the 
incoming results to a data object that is compatible with MetPy's functionality

=================
Rules of the Road
=================
> Good commenting is key: docstrings will form descriptions in user guide. 
  Comments will be edited prior to PR, but good commenting up front will make 
  this task much easier.
> Commit frequently
> Follow MetPy's coding style:
    - PEP8
    - limit line length to 95 characters
    - pay attention to style: can check this locally using flake8

=========
Structure
=========
class: 
    SynopticData

Methods - service-based:
    SynopticData.timeseries
    SynopticData.latest
    SynopticData.nearest_time
    SynopticData.precipitation
    SynopticData.example (include an example to get )

=============
Functionality
=============
Each method does the following:
    1) Build URL string for API w/ user-defined args
    2) Make request to Synoptic's API
    3) Parse/format incoming data & treat units appropriately

"""

# Dependencies
import pandas as pd
import json
import urllib.request
import numpy as np
from datetime import datetime,timezone,timedelta
#import pytz
import sys


# Need to map incompatible Synoptic units to a form that can be read by Pint.
# Simple dictionary is easier in this case than adding pint alias' since we
# return unit strings and not pint unit definitions
units_map = {'Celsius': 'degC',
             'Fahrenheit': 'degF',
             'Pascals': 'pascal',
             'Millimeters': 'millimeter',
             'Degrees': 'degree',
             'Statute_miles': 'us_statute_mile',
             'gm': 'gram'}


def append_param(url, dictionary):
    '''
    '''
    for key in dictionary.keys():
        url += key + '=' + str(dictionary[key]) + '&'
    return url


def return_stn_df(data, index):
    '''
    '''
    stid = data['STATION'][index]['STID']
    lat = data['STATION'][index]['LATITUDE']
    lon = data['STATION'][index]['LONGITUDE']
    elev = data['STATION'][index]['ELEVATION']
    meta_df = pd.DataFrame([[stid, lon, lat, elev]], columns=["stid", "lon", "lat", "elev"])
    meta_df.set_index('stid', inplace=True)

    data_out = {}
    for key in data['STATION'][index]['SENSOR_VARIABLES'].keys():
        d = data['STATION'][index]['SENSOR_VARIABLES'][key]
        for k in d.keys():
            if k == 'date_time':
                dattim = data['STATION'][index]['OBSERVATIONS'][k]
            else:
                data_out[k] = data['STATION'][index]['OBSERVATIONS'][k]
    # Generate multi-index dataframe
    multi_index = pd.MultiIndex.from_product([[stid], pd.to_datetime(dattim)], \
                                            names=["stid", "dattim"]
                                             )
    data_df = pd.DataFrame(data_out, index=multi_index)
    return data_df, meta_df


# Class instantiation - initialize w/ API token, station, time, and opt_params
class TimeSeries():
    '''
    '''
    def __init__(self, token, station={}, time={}, opt_params={}):
        '''
        Parameters
        ----------
        station: `dic`
            Dictionary of station selectors and values for data request, where dic
            keys are 'selectors' in SynopticData API parlance, and values are strings
            of values corresponding to selector. Note: specific variables should be
            requested in this dictionary.

        time: `dic`
            Temporal request variables. Valid keys are 'start' & 'end', or 'recent'.
            Values are strings or integers corresponding to the time period of interest

        opt_params: `dic`
            Optional parameters to pass to url call. This includes time-specific 
            formatting, qc selections and variable units. Dictionary keys are parameters,
            values are parameter value string

        Example
        -------
        Request air temperature and windspeed from network 1 (FAA/NWS) in state
        of Montana duing the last 2 hours. Request units of degC and m/s
        station = {'state': 'mt',
                    'network': 1,
                    'vars': 'air_temp, wind_speed'}
        time = {'recent': 120}
        opt_params = {'units': 'temp|C, speed|mps'}
        '''
        # Confirm the station & time keys are valid
        stn_keys = ['stid','state','country','nwszone','nwsfirezone','cwa',\
            'gacc','subgacc','county','vars','varsoperator','network',\
            'radius','bbox','status','complete','fields']
        if station:
            unique = set(station.keys()) - set(stn_keys)
            if len(unique)>0:
                print('Invalid station parameters: {}'.format(unique))
                sys.exit()
            else:
                self.station = station
        else:
            print('At least one station parameter is required')
            sys.exit()

        if time and set(time.keys()).intersection(set(['start','end','recent'])) == set(time.keys()):
            self.time = time
        else:
            unique = set(time.keys()) - set(['start','end','recent'])
            print('Invalid time paramaters: {}'.format(unique))
            sys.exit()

        self.token = token
        self.opt_params = opt_params


    def estimate_usage(self):
        '''
        TODO: THIS NEEDS TO BE RETHOUGHT -- OBRANGE IS APPARENTLY NOT FUNCTIONAL

        Make a call to metadata if necessary and estimate the data usage 
        from the current request
        '''
        # Build url string
        self.meta_url = 'https://api.synopticdata.com/v2/stations/metadata?'

        # Station identifier for url
        if self.station:
            meta_dic = {}    
            for key in self.station.keys():
                if key == 'vars':
                    pass
                else:
                    meta_dic[key] = self.station[key]
            self.meta_url = append_param(self.meta_url, meta_dic)
        
        # Timing for url
        if self.time:
            tz = timezone.utc
            if 'recent' in self.time.keys():
                end = datetime.now(tz)
                strt = end - timedelta(minutes=self.time['recent'])
            else:
                strt = datetime.strptime(str(self.time['start']), '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
                end = datetime.strptime(str(self.time['end']), '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
            # Check if time is too recent: if so, set request to return active
            # stations. Otherwise use obrange
            if strt > datetime.now(tz) - timedelta(days=30):
                self.meta_url += 'status=active&'
            else:
                end = end.strftime('%Y%m%d')
                strt = strt.strftime('%Y%m%d')
                self.meta_url += 'obrange='+strt+','+end+'&'
                
        # Add sensor variables -- don't confirm that the sensor variables
        # are all valid for the exact time period requested, but use this
        # as a general check of the # of variables we should expect
        self.meta_url += 'sensorvars=1&'
        
        # append token
        self.meta_url += 'token='+self.token

        # Send request to metadata service
        meta_response = urllib.request.urlopen(self.meta_url)
        metadata = json.loads(meta_response.read())
        
        # Check that the API call was valid
        if metadata['SUMMARY']['RESPONSE_CODE'] != 1:
            print('Usage estimation failed with API error:')
            print(metadata['SUMMARY']['RESPONSE_MESSAGE'])
            sys.exit()
        else:
            # Estimate data usage from the metadata return
            if 'recent' in self.time.keys():
                num_hrs = int(self.time['recent'])/60.
            else:
                start_time = pd.to_datetime(self.time['start'], format='%Y%m%d%H%M')
                end_time = pd.to_datetime(self.time['end'], format='%Y%m%d%H%M')
                dt = end_time - start_time
                num_hrs = dt.total_seconds()/(60.**2)
            num_stns = metadata['SUMMARY']['NUMBER_OF_OBJECTS']
            if 'vars' in self.station.keys():
                num_vars = len(self.station['vars'].split(","))
            else:
                # estimate number of variables based on all variables in stn 
                # database
                num_vars = 0
                for station in metadata['STATION']:
                    num_vars += len(station['SENSOR_VARIABLES'].keys())
            # usage estimates are based on 5 minute and hourly measurement intervals
            usage_min = int(np.around(num_hrs*num_stns*num_vars,-1))
            usage_max = int(np.around((60./5.)*num_hrs*num_stns*num_vars, -1))
            print("Request will return data for {} stations. \n".format(int(num_stns)))
            print("Roughly {}-{} service units (SU's) will be used".format(usage_min, usage_max))

    def request_data(self):
        #-----------------
        # Build url string
        #-----------------
        url0 = 'https://api.synopticdata.com/v2/stations/timeseries?'

        # Append the user-defined parameters to url string
        self.url = append_param(url0, self.station)
        self.url = append_param(self.url, self.time)

        # Default is to call on derived precip unless explicitly set to 0 
        if 'vars' in self.station.keys():
            if 'precip' in self.station['vars'] and 'precip' not in self.opt_params.keys():
                self.opt_params['precip'] = 1
        if self.opt_params:
            self.url = append_param(self.url, self.opt_params)
        
        # Append user token, print url string, and make data request
        self.url = self.url + 'token='+self.token
        print ('API Request url: \n {}'.format(self.url))        
        response = urllib.request.urlopen(self.url)
        self.data = json.loads(response.read())
        print('Data Request Successful!')

        # Build single dataframe with all data
        for i in range(len(self.data['STATION'])):
            if i == 0:
                data_df, meta_df = return_stn_df(self.data, i)
            else:
                df1, meta_df1 = return_stn_df(self.data, i)
                data_df = pd.concat([data_df, df1], axis=0)
                meta_df = pd.concat([meta_df, meta_df1], axis=0)
        self.data_df = data_df
        self.meta_df = meta_df


        units = self.data['UNITS']
        # Change unit variable names to be pint-compatible
        for u in units:
            if units[u] in units_map:
                units[u] = units_map[units[u]]
        # Build out a units dictionary
        unit_dic = {}
        for column in data_df.columns:
            for u_name in units.keys():
                if column.find(u_name) != -1:
                    unit = units[u_name]
            unit_dic.update({column: unit})

        return data_df, meta_df, unit_dic
