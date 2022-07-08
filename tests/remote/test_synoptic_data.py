# Copyright (c) 2017 MetPy Developers.
# Distributed under the terms of the BSD 3-Clause License.
# SPDX-License-Identifier: BSD-3-Clause
r"""Tests the operation of MetPy's Synoptic Data integration code."""

import numpy as np
import pandas as pd
import pytest

from metpy.remote import SynopticData
from metpy.units import units


station = {'stid': 'KSLC',
           'vars': ['air_temp', 'wind_speed']}

def test_timeseries():
    services = ['Timeseries','timeseries','TimeSeries']
    time = {'start': 202206010000,
            'end': 202206020000}
    for service in services:
        request = SynopticData(service, station, time)
        data, units, meta = request.request_data()
        assert len(data) > 0
        assert len(units) == 2
        assert len(meta) == 1
        assert meta.index[0] == 'KSLC'        

def test_nearest():
    services = ['Nearesttime','nearestTime','NearestTime']
    time = {'attime': 202006010000,
            'within': 120}
    for service in services:
        request = SynopticData(service, station, time)
        data, units, meta = request.request_data()
        assert len(data) > 0
        assert len(units) == 2
        assert len(meta) == 1
        assert meta.index[0] == 'KSLC'        

def test_latest():
    services = ['Latest','latest']
    time = {'within': 120}
    for service in services:
        request = SynopticData(service, station, time)
        data, units, meta = request.request_data()
        assert len(data) > 0
        assert len(units) == 2
        assert len(meta) == 1
        assert meta.index[0] == 'KSLC'        

def test_basic_precip():
    service = 'Timeseries'
    time = {'start': 202206010000,
            'end': 202206020000}
    opt_params = {'precip':1}
    request = SynopticData(service, station, time, opt_params)
    data, units, meta = request.request_data()
    assert ('precip_intervals_set_1d' in data.columns) == True
    assert ('precip_accumulated_set_1d' in data.columns) == True
    assert len(units) == 4

def test_metar():
    service = 'Timeseries'
    time = {'start': 202206010000,
            'end': 202206020000}
    station = {'stid': 'KSLC',
               'vars': ['metar']}
    request = SynopticData(service, station, time)
    data, units, meta = request.request_data()
    assert len(data['metar_set_1']) > 0
    assert type(data['metar_set_1'][0]) == str

def test_qc():
    service = 'Timeseries'
    station = {'stid': 'D6863',
               'vars': ['air_temp']}
    time = {'start': 202206010000,
            'end': 202206020000}
    opt_params = {'qc': 'on'}
    request = SynopticData(service, station, time, opt_params)
    data, units, meta, qc = request.request_data()
    assert isinstance(qc, pd.DataFrame) == True

    
