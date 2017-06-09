#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 16:27:27 2017

@author: psamtik071
"""

import pandas as pd
from urllib2 import Request, urlopen
import json
from pandas.io.json import json_normalize

url = 'https://feeds.citibikenyc.com/stations/stations.json'
df = pd.read_json(url)
request=Request(url)
response = urlopen(request)
elevations = response.read()
data = json.loads(elevations)
df = json_normalize(data['stationBeanList'])
df.head()

features = ['id', 'totalDocks', 'latitude', 'longitude',
            'stationName']
df = df[features]
df.head()

new_cols = ['id', 'docks', 'lat', 'long', 'name']
df.columns = new_cols
df.to_csv('stations.csv', index = False)