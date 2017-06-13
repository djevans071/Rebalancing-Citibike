#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 21:04:48 2017

@author: psamtik071
"""

from workflow.data import *
import pandas as pd
import os
import pdb
import holidays

year = 2015
month = 1

#create flux data table from trips dataframe and rebal dataframe


# create 'start/stop_hour' and 'start/stop_date' features
def split_off_times(df):
    df['start_hour'] = df.start_time.dt.hour
    df['stop_hour'] = df.stop_time.dt.hour
    df[['start_date', 'stop_date']] = df[['start_time', 'stop_time']].apply(lambda x: x.dt.floor('d'))
    return df

def create_fluxes(df, id_key, date_key, hour_key, fl_key):
    # id_key and hour_key are start_id/stop_id and start_date/end_date
    # start is associated with an fl_key = 'flux_out',
    # and stop is associated with fl_key = 'flux_in'
    use_cols = [id_key, date_key, hour_key, 'duration']

    flux = df.groupby([id_key, date_key, hour_key]).count()
    flux = flux.reset_index()[use_cols]
    col_dict = {'duration': fl_key,
                date_key: 'date', hour_key: 'hour',
                id_key: 'id'}
    return flux.rename(columns = col_dict)


def transform_times(df):
    # calculate approximate pickup and drop-off times for rebalancing trips
    t_start = df.start_time
    t_end = df.stop_time
    time_diff = t_start - t_end

    r_start = t_end + time_diff/3.
    r_end = t_end + time_diff*(2/3.)

    df['start_time'] = r_start
    df['stop_time'] = r_end
    return df.rename(columns = {'start_id':'stop_id', 'stop_id':'start_id'})


def merge_fluxes(df1, df2):
    # concatenate fluxes or any other dataset with the keys
    # 'id' 'date' and 'hour
    return pd.merge(df1, df2, how='outer',
                              on = ['id', 'date', 'hour']).fillna(0)


def initial_features(trips, rebals):

    # create "hour" and "date" features
    trips = split_off_times(trips)

    # create fluxes from normal trips and merge
    bikes_out = create_fluxes(trips, 'start_id', 'start_date','start_hour', 'bikes_out')
    bikes_in = create_fluxes(trips, 'stop_id', 'stop_date','stop_hour', 'bikes_in')

    merged = merge_fluxes(bikes_out, bikes_in)

    # create weekday column (Monday = 0, Sunday = 6)
    # and create other time-specific columns
    merged['dayofweek'] = merged.date.dt.weekday
    merged['month'] = merged.date.dt.month
    merged['year'] = merged.date.dt.year
    merged['is_weekday'] = merged.dayofweek.isin(range(5))
    merged['is_holiday'] = merged.date.isin(holidays.UnitedStates())


    # ----------- CALCULATING FLUXES FOR REBALANCING TRIPS ----------------

    rebals = transform_times(rebals)
    rebals = split_off_times(rebals)
    #create fluxes from rebalanced trips
    rflux_out = create_fluxes(rebals, 'start_id', 'start_date', 'start_hour', 'rbikes_out')
    rflux_in = create_fluxes(rebals, 'stop_id', 'stop_date', 'stop_hour', 'rbikes_in')

    rmerged = merge_fluxes(rflux_out, rflux_in)
    rmerged['rebal_net_flux'] = rmerged.rbikes_in - rmerged.rbikes_out
    #rmerged = rmerged.drop(['rbikes_in', 'rbikes_out'], axis=1)


    features = merge_fluxes(merged, rmerged)

    # ------ add more features ---------------------------
    # add station availability data
    avail_db = station_data(year,month)
    features = merge_fluxes(features, avail_db)
    features = features[features.year != 0.]

    #pdb.set_trace()


    return features

# --------- execute script to write features data to disk------------------

# check_cols = ['start_time', 'stop_time', 'start_id', 'stop_id', 'duration']
# trips = trip_data(year,month)[check_cols]
# rebals = rebal_data(year, month)[check_cols]
#
# out_filename = 'features_data/{}{:02}-features-data.csv'.format(year,month)
# features = initial_features(trips, rebals)
# features.to_csv(out_filename)


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
