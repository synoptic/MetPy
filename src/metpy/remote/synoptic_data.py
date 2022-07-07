# Dependencies
import pandas as pd
import json
import urllib.request
import urllib.parse
# import numpy as np
# from datetime import datetime,timezone,timedelta
import sys
import copy

from ..package_tools import Exporter

exporter = Exporter(globals())

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
    # Build query stringith
    query_string = urllib.parse.urlencode(qsp_dic)
    return query_string


def return_stn_df(data, date_format, qc_flag, service):
    '''
    '''
    meta_list = []
    for i in range(len(data)):
        # Metadata
        stid = data[i]['STID']
        try:
            lon = float(data[i]['LONGITUDE'])
        except TypeError:
            lon = None
        try:
            lat = float(data[i]['LATITUDE'])
        except TypeError:
            lat = None
        try:
            elev = float(data[i]['ELEVATION'])
        except TypeError:
            elev = None
        meta_list.append([stid, lon, lat, elev])

        # Data
        data_out = data[i]['OBSERVATIONS'].copy()
        if service == 'timeseries':
            datetime = pd.to_datetime(data_out['date_time'], format=(date_format))
            del data_out['date_time']
            multi_index = pd.MultiIndex.from_product([[stid], datetime],
                                                     names=["stid", "dattim"])
        else:
            datetime = pd.to_datetime(data_out[list(data_out.keys())[0]]['date_time'], format=(date_format))
            for key in data_out:
                data_out.update({key: data_out[key]['value']})
            multi_index = pd.MultiIndex.from_arrays([[stid], [datetime]],
                                                    names=["stid", "dattim"])
        df = pd.DataFrame(data_out, index=multi_index)

        # QC
        if qc_flag:
            qc_out = {}
            if 'QC' in data[i].keys():
                qc_out = data[i]['QC'].copy()
            stn_qc = pd.DataFrame(qc_out, index=multi_index)
        else:
            stn_qc = None

        #Concatenate as needed
        if i == 0:
            data_df = pd.DataFrame(data_out, index=multi_index)
            qc_df = pd.DataFrame(stn_qc)
        else:
            data_df = pd.concat([data_df, pd.DataFrame(data_out, index=multi_index)], axis=0)
            qc_df = pd.concat([qc_df, stn_qc], axis=0)

    meta_df = pd.DataFrame(meta_list, columns=["stid", "lon", "lat", "elev"])
    meta_df.set_index('stid', inplace=True)

    # Sort the resulting dataframe by time
    data_df.sort_index(inplace=True)

    return data_df, meta_df, qc_df


def return_precip_df(data, date_format, pmode):
    '''
    '''
    meta_list = []
    for i in range(len(data)):
        # Metadata
        stid = data[i]['STID']
        try:
            lon = float(data[i]['LONGITUDE'])
        except TypeError:
            lon = None
        try:
            lat = float(data[i]['LATITUDE'])
        except TypeError:
            lat = None
        try:
            elev = float(data[i]['ELEVATION'])
        except TypeError:
            elev = None
        meta_list.append([stid, lon, lat, elev])
        # Data: JSON formats are different for requests with 'pmode' specified
        if pmode:
            data_out = data[i]['OBSERVATIONS']['precipitation']
            if type(data_out) == list:
                df_temp = pd.DataFrame(data_out)
            else:
                df_temp = pd.DataFrame([data_out])
            df_temp.rename(columns={"total": "precipitation"}, inplace=True)
        else:
            data_out = data[i]['OBSERVATIONS']
            if type(data_out) == list:
                df_temp = pd.DataFrame(data_out)
            else:
                df_temp = pd.DataFrame([data_out])
            df_temp.rename(columns={"ob_start_time_1": "first_report",
                                    "ob_end_time_1": "last_report",
                                    "total_precip_value_1": "precipitation"}, inplace=True)
        df_temp['stid'] = stid
        #Concatenate as needed
        if i == 0:
            data_df = df_temp
        else:
            data_df = pd.concat([data_df, df_temp])
    # Remove unnecessary df columns
    if 'report_type' in data_df.columns:
        data_df.drop('report_type', axis=1, inplace=True)
    # Convert datetimes
    data_df['first_report'] = pd.to_datetime(data_df['first_report'], format=date_format)
    data_df['last_report'] = pd.to_datetime(data_df['last_report'], format=date_format)
    # Convert to multi-index -- treat both 'intervals' and 'accum_hours'
    if 'accum_hours' in data_df.columns:
        data_df.set_index(['stid','accum_hours','first_report','last_report'], inplace=True)
    elif 'interval' in data_df.columns:
        data_df.set_index(['stid','interval','first_report','last_report'], inplace=True)
    else:
        data_df.set_index(['stid','first_report','last_report'], inplace=True)
    # Build metadata df
    meta_df = pd.DataFrame(meta_list, columns=["stid", "lon", "lat", "elev"])
    meta_df.set_index('stid', inplace=True)

    return data_df, meta_df


def variable_details(data):
    '''
    '''
    position_details = {}
    derived_from_details = {}
    for i in range(len(data)):
        stid = data[i]['STID']
        sensor_dic = data[i]['SENSOR_VARIABLES']
        position = {}
        derived_from = {}
        for key in sensor_dic.keys():
            for child_key in sensor_dic[key].keys():
                if 'position' in sensor_dic[key][child_key].keys():
                    position_string = sensor_dic[key][child_key]['position']
                    if position_string != '':
                        position.update({child_key: float(position_string)})
                if 'derived_from' in sensor_dic[key][child_key].keys():
                    derived_from.update({child_key: sensor_dic[key][child_key]['derived_from']})
        position_details.update({stid: position})
        derived_from_details.update({stid: derived_from})
    return position_details, derived_from_details

def case_insensitive(input_dic):
    '''
    '''
    lower_case_dic = {}
    for k,v in input_dic.items():        
        try:
            lower_case_dic[k.lower()] = v.lower()
        except AttributeError:
            lower_case_dic[k.lower()] = v
    return lower_case_dic

@exporter.export
class SynopticData():
    """
    Class for requesting data from
    `Synoptic Data API Services <https://developers.synopticdata.com/mesonet/>`_


    Attributes
    ----------
    service : str
            Synoptic service to query. 'TimeSeries', 'Latest', 'NearestTime', or 'Precip'
    station : dic
            Dictionary of station selectors and values for data request, where dic
            keys are 'selectors' in SynopticData API parlance, and values are strings
            of values corresponding to selector. Note: specific variables should be
            requested in this dictionary.
    time : dic (optional)
            Temporal request variables. Keys correspond to valid time selectors
            from each service. Values are strings or integers corresponding to the
            time period of interest
    opt_params : dic (optional)
            Optional parameters to pass to url call. This includes time-specific
            formatting, qc selections and variable units. Dictionary keys are parameters,
            values are parameter value string
    token : str (optional)
            User token to access Synoptic Data's API
    """
    def __init__(self, service, station, time={}, opt_params={}, token=''):
        # Eliminate any case sensitivity
        service = service.lower()
        station = case_insensitive(station)
        time = case_insensitive(time)
        opt_params = case_insensitive(opt_params)

        # Confirm the service is valid
        if service == 'timeseries':
            self.valid_time_keys = ['start', 'end', 'recent']
            self.url0 = "https://api.synopticdata.com/v2/stations/timeseries?"
            self.service = service
        elif service == 'latest':
            self.valid_time_keys = ['within', 'minmax', 'minmaxtype']
            self.url0 = "https://api.synopticdata.com/v2/stations/latest?"
            self.service = service
        elif service == 'nearesttime':
            self.valid_time_keys = ['within', 'attime']
            self.url0 = "https://api.synopticdata.com/v2/stations/nearesttime?"
            self.service = service
        elif service == 'precip':
            self.valid_time_keys = ['start', 'end', 'recent']
            self.url0 = "https://api.synopticdata.com/v2/stations/precip?"
            self.service = service
        else:
            sys.exit('Invalid Service')
        self.station = station
        self.time = time
        if token != '':
            self.token = token
        else:
            # Grab a token from the MetPy user account
            try:
                with urllib.request.urlopen("https://api.synopticdata.com/metpy-token/v1") as response:
                    self.token = response.read().decode("utf-8")
            except:
                raise RuntimeError('Error fetching token. Contact Synoptic support')

        self.opt_params = opt_params
        # Store valid station keys
        self.valid_station_keys = ['stid','stids','state','country','nwszone','nwsfirezone','cwa',\
                            'gacc','subgacc','county','vars','varsoperator','network',\
                            'radius','bbox','status','complete','fields','networkimportance',\
                            'spacing']

    def request_data(self):
        """Build url string, send API data request, and return parsed data

        Returns
        -------
        data_df : Pandas DataFrame
                Data columns multi-indexed by station and dattim
        units_dic : dic
                Units (string value) for each data column (key) in data_df
        meta_df : Pandas DataFrame
                Station latitude, longitude, and elevation. Indexed by station id
        qc_df : Pandas dataframe (optional)
                qc_flags for each variable, multi-indexed in the same fashion as
                data_df. qc_df returns if 'qc_flag' or 'qc' are set to 'on' in
                opt_params. For qc details, see https://developers.synopticdata.com/about/qc/
        """
        # Confirm the service hasn't been changed:
        service_url = self.url0.split('/')[-1]
        print(f'Service url is {service_url}')

        if service_url[:-1] != self.service.lower():    
            raise ValueError(f'Instantiate a new class for a different API service')
        
        # Catch invalid station requests
        if self.station:
            invalid_stns = set(self.station.keys()) - set(self.valid_station_keys)
            if len(invalid_stns) > 0:
                raise ValueError(f'{invalid_stns} is not a valid station selector')
            else:
                self.station = copy.deepcopy(self.station)
        else:
            raise ValueError(f'At least one station selector is required')
        
        # Catch invalid time keys
        invalid_time = set(self.time.keys()) - set(self.valid_time_keys)
        if len(invalid_time) > 0:
            raise ValueError(f'{invalid_time} is not a valid time parameter')

        # Default is to call on derived precip in TimeSeries service unless explicitly set to 0
        if 'vars' in self.station.keys() and self.service == 'timeseries':
            if 'precip' in self.station['vars'] and 'precip' not in self.opt_params.keys():
                self.opt_params['precip'] = 1

        # If this is a Precip request, check to see if pmode is defined
        if 'pmode' in self.opt_params.keys():
            self.pmode = 1
        else:
            self.pmode = None

        # Check if there is a qc_flag request
        if ('qc_flag','on') in self.opt_params.items():
            self.qc_flag = 1
        elif ('qc','on') in self.opt_params.items() and ('qc_flag','off') not in self.opt_params.items():
            self.qc_flag = 1
        else:
            self.qc_flag = None

        # Build single dictionary for url query string
        qsp_dic = self.station.copy()
        if self.time:
            qsp_dic.update(self.time)
        if self.opt_params:
            qsp_dic.update(self.opt_params)
        qsp_dic.update({'token': self.token})
        query_string = build_query_string(qsp_dic)
        self.url = self.url0 + query_string

        #print url string, and make data request
        print ('API Request url: \n {}'.format(self.url))
        with urllib.request.urlopen(self.url) as response:
            self.data = json.loads(response.read())        

        # Set time format
        if 'timeformat' in self.opt_params.keys():
            time_format = self.opt_params['timeformat']
        else:
            time_format = '%Y-%m-%d %H:%M'

        # If request returns 0 results, print as such & exit
        if self.data['SUMMARY']['RESPONSE_CODE'] != 1:
            raise RuntimeError(self.data['SUMMARY']['RESPONSE_MESSAGE'])

        # Else build out data & metadata dataframes, and units dic
        else:
            if self.service != 'precip':
                data_df, meta_df, qc_df = return_stn_df(self.data['STATION'], time_format, self.qc_flag, self.service)
                position, derived_from = variable_details(self.data['STATION'])
                self.variable_position = position
                self.variable_derived_from = derived_from
            else:
                data_df, meta_df = return_precip_df(self.data['STATION'], time_format, self.pmode)
            self.data_df = data_df
            self.meta_df = meta_df

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

            if self.qc_flag:
                # If there are no columns in the qc df, there are no flags. Reset
                # to None
                if len(qc_df.columns) == 0:
                    qc_df = None
                self.qc_df = qc_df
                return data_df, unit_dic, meta_df, qc_df
            else:
                return data_df, unit_dic, meta_df
