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
classes:
    TimeSeries
    Latest
    Nearest

=============
Functionality
=============
Each class does the following:
    1) Build URL string for API w/ user-defined args
    2) Make request to Synoptic's API
    3) Parse/format incoming data & treat units appropriately

"""

# Dependencies
import pandas as pd
import json
import urllib.request
import urllib.parse
import numpy as np
from datetime import datetime,timezone,timedelta
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
             'gm': 'gram',
             'Inches': 'inch',
             'Miles/hour': 'mile/hour',
             'ug/m3': 'ug/m**3',
             'ft3/s': 'ft**3/s'}


def build_query_string(qsp_dic):
    # Convert any lists to comma-separated strings
    for k in qsp_dic.keys():
        if isinstance(qsp_dic[k], list):
            qsp_dic[k] = ','.join(map(str,qsp_dic[k]))
    # Build query string
    query_string = urllib.parse.urlencode(qsp_dic)
    return query_string


def return_stn_df(data, stn_index, date_format, qc_flag):
    '''
    '''
    # Site meta
    stid = data['STATION'][stn_index]['STID']
    lat = data['STATION'][stn_index]['LATITUDE']
    lon = data['STATION'][stn_index]['LONGITUDE']
    elev = data['STATION'][stn_index]['ELEVATION']
    meta_df = pd.DataFrame([[stid, lon, lat, elev]], columns=["stid", "lon", "lat", "elev"])
    meta_df.set_index('stid', inplace=True)

    # Site data
    data_out = data['STATION'][stn_index]['OBSERVATIONS'].copy()
    datetime = pd.to_datetime(data_out['date_time'], format=(date_format))
    del data_out['date_time']
    multi_index = pd.MultiIndex.from_product([[stid], datetime], names=["stid", "dattim"]
                                             )
    data_df = pd.DataFrame(data_out, index=multi_index)

    # Build dataframe of qc_flags if qc_flags were requested
    if qc_flag == 1:
        qc_out = {}
        if 'QC' in data['STATION'][stn_index].keys():
            qc_out = data['STATION'][stn_index]['QC'].copy()
        qc_df = pd.DataFrame(qc_out, index=multi_index)
    else:
        qc_df = None
    return data_df, meta_df, qc_df


def variable_details(data, stn_index):
    sensor_dic = data['STATION'][stn_index]['SENSOR_VARIABLES']
    position = {}
    derived_from = {}
    for key in sensor_dic.keys():
        for child_key in sensor_dic[key].keys():
            if 'position' in sensor_dic[key][child_key].keys():
                position.update({child_key: float(sensor_dic[key][child_key]['position'])})
            if 'derived_from' in sensor_dic[key][child_key].keys():
                derived_from.update({child_key: sensor_dic[key][child_key]['derived_from']})
    return position, derived_from


# Class instantiation - initialize w/ API token, station, time, and opt_params
class TimeSeries():
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
        # Confirm the station keys are valid
        stn_keys = ['stid','state','country','nwszone','nwsfirezone','cwa',\
            'gacc','subgacc','county','vars','varsoperator','network',\
            'radius','bbox','status','complete','fields']
        if station:
            invalid_stns = set(station.keys()) - set(stn_keys)
            if len(invalid_stns) > 0:
                sys.exit('Invalid station parameters: {}'.format(invalid_stns))
            else:
                self.station = station
        else:
            sys.exit('At least one station parameter is required')

        # Confirm the time keys are valid
        if time:
            invalid_time = set(time.keys()) - set(['start', 'end', 'recent'])
            if len(invalid_time) > 0:
                sys.exit('Invalid time parameters: {}'.format(invalid_time))
            else:
                self.time = time
        else:
            sys.exit("time parameters of 'start' and 'end' or 'recent' are required")
        self.token = token
        self.opt_params = opt_params
        self.url0 = "https://api.synopticdata.com/v2/stations/timeseries?"


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
            #NOTE: append_param is now deprecated
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
                self.meta_url += 'obrange=' + strt + ',' + end + '&'
                
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
        '''
        Build url string, send data request, and return data, metadata, and units

        Returns:
            data_df: Pandas dataframe with data columns, multi-indexed by station
                     and dattim
            meta_df: Pandas dataframe with station latitude, longitude, and elev
            units: Dictionary of units for each data variable in data_df
        '''
        # Default is to call on derived precip unless explicitly set to 0 
        if 'vars' in self.station.keys():
            if 'precip' in self.station['vars'] and 'precip' not in self.opt_params.keys():
                self.opt_params['precip'] = 1

        # Check if there is a qc_flag request
        if ('qc_flag','on') in self.opt_params.items():
            qc_flag = 1
            self.qc_flag = 1
        elif ('qc','on') in self.opt_params.items() and ('qc_flag','off') not in self.opt_params.items():
            qc_flag = 1
            self.qc_flag = 1
        else:
            qc_flag = 0
            self.qc_flag = 0

        # Build single dictionary for url query string
        qsp_dic = self.station
        if self.time:
            qsp_dic.update(self.time)
        if self.opt_params:
            qsp_dic.update(self.opt_params)
        qsp_dic.update({'token': self.token})
        query_string = build_query_string(qsp_dic)
        self.url = self.url0 + query_string

        #print url string, and make data request
        print ('API Request url: \n {}'.format(self.url))
        response = urllib.request.urlopen(self.url)
        self.data = json.loads(response.read())

        # Set time format
        if 'timeformat' in self.opt_params.keys():
            time_format = self.opt_params['timeformat']
        else:
            time_format = '%Y-%m-%d %H:%M'

        # If request returns 0 results, print as such & exit
        if self.data['SUMMARY']['RESPONSE_CODE'] != 1:
            sys.exit(self.data['SUMMARY']['RESPONSE_MESSAGE'])
        # Else build out data & metadata dataframes, and units dic
        else:
            for i in range(len(self.data['STATION'])):
                if i == 0:
                    data_df, meta_df, qc_df = return_stn_df(self.data, i, time_format, qc_flag)
                    position, derived_from = variable_details(self.data, i)
                else:
                    df1, meta1, qc1 = return_stn_df(self.data, i, time_format, qc_flag)
                    data_df = pd.concat([data_df, df1], axis=0)
                    meta_df = pd.concat([meta_df, meta1], axis=0)
                    position1, derived_from1 = variable_details(self.data, i)
                    position.update(position1)
                    derived_from.update(derived_from1)
                    if qc_flag == 1:
                        qc_df = pd.concat([qc_df, qc1], axis=0)
            self.data_df = data_df
            self.meta_df = meta_df
            self.variable_position = position
            self.variable_derived_from = derived_from

            # Change unit variable names to be pint-compatible & build out dic
            units = self.data['UNITS']
            for u in units:
                if units[u] in units_map:
                    units[u] = units_map[units[u]]
            unit_dic = {}
            for column in data_df.columns:
                for u_name in units.keys():
                    if column.find(u_name) != -1:
                        unit = units[u_name]
                unit_dic.update({column: unit})

            if qc_flag == 1:
                # If there are no columns in the qc df, there are no flags. Reset
                # to None
                if len(qc_df.columns) == 0:
                    qc_df = None
                self.qc_df = qc_df
                return data_df, meta_df, qc_df, unit_dic
            else:
                return data_df, meta_df, unit_dic
