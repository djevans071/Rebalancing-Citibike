#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 16:19:11 2017

@author: psamtik071
"""

from workflow.data import trip_data
import os


for year in xrange(2017,2018):
    for month in xrange(1,13):

        basepath = 'tripdata/'
        to_filename = '{}{:02}-citibike-tripdata.csv'.format(year, month)
        path = basepath + to_filename
        print "cleaning trips from {}".format(path)

        if os.path.exists(to_filename):
            print "{} already exists".format(to_filename)
            pass
        else:

            df = pd.read_csv(path)

            # rename columns
            new_cols = ['duration', 'start_time', 'stop_time', 'start_id', 'start_name',
               'start_lat', 'start_long', 'stop_id', 'stop_name', 'stop_lat',
               'stop_long', 'bike_id', 'user_type', 'birth_year', 'gender']

            df.columns = new_cols
            df.start_time = pd.to_datetime(df.start_time, format = '%Y-%m-%d %H:%M:%S')
            df.stop_time = pd.to_datetime(df.stop_time, format = '%Y-%m-%d %H:%M:%S')

            df.to_csv(to_filename,index = None)

