#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 13:02:10 2017

@author: psamtik071
"""

# create rebalancing datasets from trips datasets and save to rebaltrips\

from processing.data import *
import os

month = 3
year = 2017

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
# (REMEMBER THAT REBAL EVENTS USE STOP_TIME AS THE STARTING TIME)

to_folder = 'rebals/'


for year in xrange(2017,2018):
    for month in xrange(1,3):

        to_filename = '{}{:02}-rebal-data.csv'.format(year, month)
        path = to_folder + to_filename
        print "extracting rebalancing trips to {}".format(path)

        if os.path.exists(path):
            print "{} already exists".format(path)
            pass
        else:
            try:
                rebal_from_trips(trip_data(year, month)).to_csv(path, index = None)
            except IOError:
                print '{} does not exist'.format(path)
