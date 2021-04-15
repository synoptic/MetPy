"""
Methods to build url strings to call data from Synoptic API

"""
# Series of methods to generate string snippets for url call
# ----------------------------------------------------------
def station_str_snippet(stn):
    '''
    stn = {'selector': str,
           'value': list or str,
           'opt_selector': str,
           'opt_value': str,
           'status': 'active'/'inactive',
           'complete': 1/0}

           'opt_selector': optional arguments under 'selector'
                           e.g. radius can have an optional 'limits', or 
                           'county' can have an optional 'state'
           'opt_value': value assocaited with 'opt_selector'
                        e.g. 10 associated with 'limits' limits to 10 stns
           'showemptystations': 'on'/'off'}
    '''
    # Convert stn to appropriate syntax for API call:
    # -----------------------------------------------
    # This joining should support all 'selector' options with multiple values
    if stn:
        if type(stn['value']) == list:
            stn_str = '&' + stn['selector'] + '=' + ','.join(stn['value'])
        else:
            stn_str = '&' + stn['selector'] + '=' + stn['value']
    else:
        stn_str = ''
        print('No stations/network selected. API will return all')

    if 'opt_selector' in stn.keys():
        stn_str = "&".join((stn_str, "=".join((stn['opt_selector'], stn['opt_value']))))

    if 'status' in stn.keys():
        stn_str = "&".join((stn_str, 'status=' + stn['status']))

    if 'complete' in stn.keys():
        stn_str = "&".join((stn_str, 'complete=' + str(stn['complete'])))
    
    if 'fields' in stn.keys():
        if type(stn['fields']) == list:
            stn_str = "&".join((stn_str, 'fields=' + ",".join(stn['fields'])))
        else:
            stn_str = "&".join((stn_str, 'fields=' + stn['fields']))
    
    return stn_str

def variable_str_snippet(vars):
    '''
    vars = {'vars': ['list', 'of', 'variables'],
            'varsoperator': 'and'/'or',
            'units': 'metric', 'english', custom (['temp=C', 'speed=mph']),
            'hfmetars': '1'/'0',
            'precip': '1'/'0' (only supported for timeseries calls)}
            
    '''
    # Convert 'vars' list to appropriate syntax for API call
    if vars:
        var_str = '&vars=' + ','.join(vars['vars'])
    else:
        var_str = ''
        print('No variables listed, API will return all')

    if 'vars_operator' in vars.keys():
        var_str = "&".join((var_str, 'varsoperator'+vars['varsoperator']))

    if 'units' in vars.keys():
        if type(vars['units']) == list:
            var_str = "&".join((var_str, 'units=' + ",".join(vars['units'])))
        else:
            var_str = "&".join((var_str, 'units'+vars['units']))

    if 'hfmetars' in vars.keys():
        var_str = "&".join((var_str, 'hfmetars'+str(vars['hfmetars'])))

    return var_str

def response_str_snippet(response):
    '''
    response = {'obtimezone' : 'local'/'UTC',
                'timeformat' : ,
                'output'     : 'json'}
    '''
    valid_params = ['obtimezone', 'timeformat', 'output']
    response_str = ''

    for key in response:
        if key not in valid_params:
            print('Invalid optional parameter: {}'.format(key))
        else:
            response_str = "&".join((response_str, key + '=' + response[key]))
    
    return response_str

def qc_str_snippet(qc_dic):
    '''
    qc = {'qc'             : 'on',
          'qc_remove_data' : 'on',
          'qc_flags'       : 'off',
          'qc_checks'      : 'sl_range_check'}
    '''    
    valid_params = ['qc', 'qc_remove_data', 'qc_flags', 'qc_checks']
    qc_str = ''

    for key in qc_dic:
        if key not in valid_params:
            print('Invalid optional parameter: {}'.format(key))
        else:
            qc_str = "&".join((qc_str, key + '=' + qc_dic[key]))

    return qc_str


#-------------------------
# Method 1: time series
#-------------------------
def api_str_timeseries(stn={}, vars={}, response={}, qc={}, time={}, api_root="https://api.synopticdata.com/v2/", \
                   api_token='here'): 
    '''
    Build url string for Synoptic api call based on obs, time, optional parameters, and 
    API details (root and user token).

    Formatting details and variables can be found at:
        https://developers.synopticdata.com/mesonet/v2/stations/timeseries/
    
    INPUTS:
        stn         - Dictionary specifying key/value pair for site/network details, 
                      and variables of interest
                      e.g.  Select all obs in Montana
                            obs = {'selector' : 'state',
                                  'value'    : 'mt'} 
                            -- or --
                            Select all obs w/in 60 mile radius of lon=-114, lat=45
                            obs = {'selector' : 'radius',
                                   'value'    : ['-114.','45','60']}                                   
        vars        - list of variables to call. 
                       e.g. ['air_temp', 'snow_depth']}
        time        - Dictionary specifying the 'start' and 'end' -OR- 'recent'
                      e.g. time = {'start' : 202101010000,
                                   'end'   : 202101310000}
                          -- or --
                          time = {'recent' : 120}
        opt_params  - Dictionary specifying any of the optional parameters found on the API
                      website. If empty, revert to defaults.
                      e.g. opt_params = {'obtimezone' : 'local',
                                         'qc'         : 'off'}
    
    OUTPUTS:
        URL string specifying API call
    '''
    # Build strings
    stn_str = station_str_snippet(stn)
    var_str = variable_str_snippet(vars)
    qc_str = qc_str_snippet(qc)
    response_str = response_str_snippet(response)

    # Time string
    time_str=''
    if time:
        for key in time.keys():
            time_str = time_str+'&'+key+'='+time[key]
    else:
        print('No Time Selected! API will not return')

    # Token String
    token_str = '&token='+api_token

    # Build url for API call
    url = api_root + 'stations/timeseries?' + \
        stn_str + \
        var_str + \
        time_str + \
        qc_str + \
        response_str + \
        token_str
    print ('URL: {}'.format(url))

    return url


#-----------------------
# Method 2: Latest
#-----------------------
def api_str_latest(stn={}, vars={}, response={}, qc={}, latest={}, api_root="https://api.synopticdata.com/v2/", \
                   api_token='yo', opt_params={}): 
    '''
    Build url string for Synoptic api call based on inputs for station/network, time, optional parameters, and 
    API details (root and user token).

    Formatting details and variables can be found at:
        https://developers.synopticdata.com/mesonet/v2/stations/latest/
    
    INPUTS:
        stn         - Dictionary specifying key/value pair for site/network details, 
                      and metadata
                      e.g.  Select all obs in Montana
                            stn = {'selector' : 'state',
                                  'value'    : 'mt'} 
                            
                            Select all obs w/in 60 mile radius of lon=-114, lat=45
                            stn = {'selector' : 'radius',
                                   'value'    : ['-114.','45','60']}
        vars        - dictionary specifying variables to call, and optional 'varsoperator'. 
                      e.g. vars = {'vars': ['air_temp', 'pressure'],
                                   'varsoperator': 'and'}
        response    - dictionary specifying output arguments (obtimezone, timeformat, output)
                      e.g. response = {'obtimezone': 'local',
                                       'timeformat': '%m/%d/%Y at %H:%M',
                                       'output': 'csv'}
        qc          - dictioanry specifying qc arguments
                      e.g. qc = {}
        latest_args - Dictionary specifying variables associated with 'Latest' API call: 
                      ['within', 'minmax', 'minmaxtype']
                      e.g. latest_vars = {'within' : '60'}
        opt_params  - Dictionary specifying any of the optional parameters found on the API
                      website. If empty, revert to defaults.
                      e.g. opt_params = {'obtimezone' : 'local',
                                        'qc'         : 'off'}
    
    OUTPUTS:
        URL string specifying API call
    '''
    # Build strings
    stn_str = station_str_snippet(stn)
    var_str = variable_str_snippet(vars)
    qc_str = qc_str_snippet(qc)
    response_str = response_str_snippet(response)

    # 'Latest' variable string
    latest_str=''
    for key in latest.keys():
        latest_str = latest_str+'&'+key+'='+latest[key]

    # Token String
    token_str = '&token='+api_token

    # Build url for API call
    url = api_root + 'stations/latest?' + \
        stn_str + \
        var_str + \
        qc_str + \
        response_str +\
        latest_str + \
        token_str
    print ('URL: {}'.format(url))

    return url


#---------------------------
# TODO: Method 3: Nearest time
#---------------------------
#region
#Required: TOKEN, ATTIME, WITHIN

# STATION SELECTION (stid, state, country, nwszone, nwsfirezone, \
#                    cwa, gacc, subgacc, county, vars, varsoperator, radius, bbox)
# STATION OPTIONS    (status, complete, fields)
# TIME               (start & end, or recent)
# OPTIONAL PARAMS    (obtimezone, showemptystations, units, precip, hfmetars)
# TIME FORMAT        (strftime expression)
# OUTPUT FORMAT      (csv, json, xml, geojson)
# QUALITY CONTROL    (qc, qc_remove_data, qc_flags, qc_checks)
#endregion

#------------------------- 
# TODO: Method 4: Metadata
#-------------------------