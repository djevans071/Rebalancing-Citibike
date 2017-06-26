#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import pandas as pd
from bs4 import BeautifulSoup
from urllib2 import Request, urlopen
import json
from pandas.io.json import json_normalize
from datetime import datetime
import holidays
import pickle

def get_live_temp():
    # scrape live weather data site to get current temperature
    url = 'http://w1.weather.gov/data/obhistory/KNYC.html'
    page = urlopen(url).read()
    soup = BeautifulSoup(page, 'lxml')

    live_temp = 0
    for tr in soup.find_all('tr')[7:8]:
        tds = tr.find_all('td')
        live_temp += float(tds[6].text)
    return live_temp

def get_live_station_data(dock_id):
    # returns live station data from citibike json feed
    url = 'https://feeds.citibikenyc.com/stations/stations.json'
    df = pd.read_json(url)
    request = Request(url)
    response = urlopen(request)
    elevations = response.read()
    data = json.loads(elevations)
    df = json_normalize(data['stationBeanList'])

    features = ['id', 'lastCommunicationTime', 'totalDocks',
                'latitude', 'longitude', 'stationName', 'availableBikes', 'availableDocks', 'statusKey', 'statusValue']
    return df[df.id == dock_id][features]

def gather_params(station_number):
    # get live temperature data
    temp = get_live_temp()
    # get live station data
    now_station = get_live_station_data(station_number)
    now = datetime.now()

    # cols = ['id', 'long', 'lat', 'hour', 'dayofweek',
            #  'month', 'is_weekday', 'is_holiday',
            #  'precip', 'temp', 'pct_avail_bikes', 'pct_avail_docks']

    X = pd.DataFrame()
    X['id'] = now_station.id
    X['long'] = now_station.longitude
    X['lat'] = now_station.latitude
    X['hour'] = now.hour
    X['dayofweek'] = now.weekday()
    X['month'] = now.month

    if now.weekday() in range(0,5):
        X['is_weekday'] = 1
    else:
        X['is_weekday'] = 0

    if now.date() in holidays.UnitedStates():
        X['is_holiday'] = 1
    else:
        X['is_holiday'] = 0

    X['is_holiday'] = 0
    X['precip'] = 0
    X['temp'] = temp
    X['pct_avail_bikes'] = now_station.availableBikes / now_station.totalDocks
    X['pct_avail_docks'] = now_station.availableDocks / now_station.totalDocks

    return X

def load_regressor():
    with open('regressor.pkl', "r") as reg_infile:
        reg = pickle.load(reg_infile)
    return reg

def apply_model(station_number, reg):
    X = gather_params(station_number)
    return reg.predict(X)
