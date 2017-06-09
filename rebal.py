#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 13:02:10 2017

@author: psamtik071
"""

from workflow.data import *
import os

month = 3
year = 2017
#trips = trip_data(year, month)
#columns to shift by
shift_cols = ['stop_id','stop_time', 'stop_long', 'stop_lat', 'stop_name']

'''
these functions are moved to workflow/data.py

def shift_cond(bike_df):
    bike_df[shift_cols] = bike_df[shift_cols].shift(1)
    return bike_df[bike_df.start_id != bike_df.stop_id]


def rebal_from_trips(trips):
    trips = trips.sort_values(['bike_id', 'start_time'])
    rebal_df = trips.groupby('bike_id').apply(shift_cond).dropna()
    rebal_df.index = rebal_df.index.droplevel('bike_id')
    return rebal_df
'''

# track rebalancing events
#(REMEMBER THAT REBAL EVENTS USE STOP_TIME AS THE STARTING TIME)

to_folder = 'rebaltrips/'

#rebal_from_trips(trip_data(year, month)).to_csv(to_folder + to_filename,
 #               index = None)

for year in xrange(2015,2017):
    for month in xrange(1,13):

        to_filename = '{}{:02}-rebal-data.csv'.format(year, month)
        path = to_folder + to_filename
        print "extracting rebalancing trips from {}".format(path)

        if os.path.exists(path):
            print "{} already exists".format(path)
            pass
        else:
            try:
                rebal_from_trips(trip_data(year, month)).to_csv(path, index = None)
            except IOError:
                print '{} does not exist'.format(path)
