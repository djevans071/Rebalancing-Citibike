#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 13:02:10 2017

@author: psamtik071
"""

import requests, zipfile
import StringIO
import datetime
import pandas as pd
import numpy as np
from workflow.data import *

month = 3
year = 2017
trips = trip_data(year, month)

bike_list = trips.bike_id.unique()
# collect rebal events for the first 100 bikes
trips = trips.sort_values(['bike_id', 'start_time'])
#cols = ['start_id','stop_id', 'start_time', 'stop_time', 'bike_id']
#trips = trips[trips.bike_id.isin(bike_list[:100])]

# track rebalancing events
#(REMEMBER THAT REBAL EVENTS USE STOP_TIME AS THE STARTING TIME)


shift_cols = ['stop_id','stop_time', 'stop_long', 'stop_lat', 'stop_name']

def shift_cond(bike_df):
    bike_df[shift_cols] = bike_df[shift_cols].shift(1)
    return bike_df[bike_df.start_id != bike_df.stop_id]

rebal_df = trips.groupby('bike_id').apply(shift_cond).dropna()
rebal_df.index = rebal_df.index.droplevel('bike_id')
rebal_df.to_csv('{}{:02}-rebal-data.csv'.format(year, month), index = None)
