#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 21:04:48 2017

@author: psamtik071
"""

from workflow.data import *
import pandas as pd
import pdb

year = 2015
month = 1

#create flux data table from trips dataframe and rebal dataframe
rabals_folder = 'rebals/'

trips_file = 'tripdata/{}{:02}-citibike-tripdata.csv'.format(year,month)
rebals_file = 'rebals/{}{:02}-rebal-data.csv'.format(year,month)

check_cols = ['start_time', 'stop_time', 'start_id', 'stop_id']

#load data
trips = trip_data(year,month)[check_cols]
rebals = rebal_data(year, month)[check_cols]


def create_fluxes(df, id_key, time_key, fl_key):
    # id_key and time_key are start_id/stop_id and start_time/end_time
    # start is associated with an fl_key = 'flux_out',
    # and stop is associated with fl_key = 'flux_in'
    flux = df[[id_key, time_key]].groupby([df[time_key].dt.to_period('H'), id_key]).count()
    flux = flux[time_key].fillna(0).unstack(level=0).stack()
    flux = flux.reset_index().set_index(time_key)
    flux.index.name = 'date'
    return flux.rename(columns = {0:fl_key, id_key:'id'}).reset_index()

def merge_fluxes(df1, df2):
    return pd.merge(df1, df2, how='outer',
                    on = ['date', 'id']).fillna(0)

# create fluxes from normal trips
bikes_out = create_fluxes(trips, 'start_id', 'start_time', 'bikes_out')
bikes_in = create_fluxes(trips, 'stop_id', 'stop_time', 'bikes_in')

merged = merge_fluxes(bikes_out, bikes_in)

# create weekday column with values 1 if weekday and 0 if weekend
wkd_cond = lambda x: 1 if True else 0
merged['weekday'] = merged.date.dt.weekday.isin(range(5)).apply(wkd_cond)
merged['hour'] = merged.date.dt.hour

# ----------- CALCULATING FLUXES FOR REBALANCING TRIPS ----------------

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

rebals = transform_times(rebals)
#create fluxes from rebalanced trips
rflux_out = create_fluxes(rebals, 'start_id', 'start_time', 'rflux_out')
rflux_in = create_fluxes(rebals, 'stop_id', 'stop_time', 'rflux_in')

rmerged = merge_fluxes(rflux_out, rflux_in)
rmerged = rmerged.drop(['rflux_in', 'rflux_out'], axis=1)

# -------------- OTHER FEATURES -------------------------------------

features = merge_fluxes(merged, rmerged)
