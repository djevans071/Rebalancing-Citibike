#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 21:04:48 2017

@author: psamtik071
"""

from processing.data import *
from processing.features import split_off_times,
                                create_fluxes,
                                transform_times,
                                merge_fluxes,
                                initial_features
import pandas as pd
import os
import pdb
import holidays

# --------- execute script to write features data to disk------------------

# check_cols = ['start_time', 'stop_time', 'start_id', 'stop_id', 'duration']
# trips = trip_data(year,month)[check_cols]
# rebals = rebal_data(year, month)[check_cols]
#
# out_filename = 'features_data/{}{:02}-features-data.csv'.format(year,month)
# features = initial_features(trips, rebals)
# features.to_csv(out_filename)

if __name__ == '__main__':
    year = 2015
    month = 1

    for year in xrange(2015,2018):
        for month in xrange(1,13):

            out_filename = 'features_data/{}{:02}-features-data.csv'.format(year,month)
            print "extracting features for {}-{:02}".format(year, month)


            if os.path.exists(out_filename):
                print "{} already exists".format(out_filename)
                pass
            else:
                try:
                    check_cols = ['start_time', 'stop_time',
                          'start_id', 'stop_id', 'duration']
                    trips = trip_data(year,month)[check_cols]
                    rebals = rebal_data(year, month)[check_cols]
                    features = initial_features(trips, rebals)
                    features.to_csv(out_filename, index = None)
                except IOError:
                    print '\tInput data does not exist for {}-{:02}'.format(year, month)
