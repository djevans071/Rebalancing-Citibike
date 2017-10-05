#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 22:15:40 2017

@author: psamtik071
"""

"""
Routines for extracting features from data
"""

import pandas as pd

# make sure tot_docks > 0 (especially when calculating bikes available)
def bulk_query(year):
    # query all bikes after or before a certain time
    if year == 2015:
        date_string = "< '2016-03-01'"
    if year == 2016:
        date_string = ">= '2016-03-01'"

    query = """
        SELECT a.id, a.date, a.hour, bikes_out, bikes_in, dayofweek, month,
            is_weekday, is_holiday, rebal_net_flux, tot_docks, avail_bikes,
            avail_docks, precip, snow, temp, long, lat
        FROM features a
        LEFT JOIN weather b ON a.date = b.date AND a.hour = b.hour
        LEFT JOIN stations c on a.id=c.id
        WHERE a.date {} AND tot_docks > 0
        ORDER BY a.id, a.date, a.hour;
    """.format(date_string)
    return query

def strip_unused_stations(df, station_list):
    # strip away unused stations that are not in station_list
    return df[df.id.isin(station_list)]


def make_categorical(df, cols):
    for col in cols:
        df[col] = df[col].astype('category')
    return df

def flux_conditions(x, threshold = 0.2):
    # for x in pct_flux, set the following parameters:
    # if x > 0.2 ---> flux_type 1 (rebalance down -- remove bikes)
    # if x < -0.2 ---> flux_type -1 (rebalance up -- add bikes)
    # if abs(x) <= 0.2 ---> flux_type 0 (don't rebalance)
    if x > abs(threshold):
        return x
    elif x < -abs(threshold):
        return x
    else:
        return 0

def temp_conditions(x):
    # temperature categories
    if x > 80.:
        return 80 #hot
    elif (x > 60.) & (x <= 80.):
        return 70 #mild
    elif (x > 40.) & (x <= 60.):
        return 50 #chilly
    else:
        return 30 #cold

def precip_conditions(x):
    # precipitation categories
    if x > 0.10:
        return 1
    else:
        return 0

def merge_by_date(df1, df2):
    return pd.merge(df1, df2, how = 'left', on = 'date')

def make_lagged_fluxes(df):
    '''
    create a daily avg flux column and shift it to get yesterday's flux for a given date.

    also with weekly fluxes
    '''
    mean_daily_flux = df.groupby('date').mean().flux
    mean_yesterday_flux = mean_daily_flux.shift(1).reset_index()
    mean_lastweek_flux = mean_daily_flux.shift(7).reset_index()

    mean_daily_flux = mean_daily_flux.reset_index().rename(columns = {'flux': 'mean_flux'})
    mean_yesterday_flux = mean_yesterday_flux.rename(columns = {'flux': 'yest_flux'})
    mean_lastweek_flux = mean_lastweek_flux.rename(columns = {'flux': 'last_week_flux'})

    dfs = [df, mean_daily_flux, mean_yesterday_flux, mean_lastweek_flux]
    return reduce(merge_by_date, dfs)

def new_features(df):

    df['hour'] = df['hour'].astype(int)

    # turn strings 'True' and 'False' into 1 and 0
    string_dict = {'True': 1, 'False':0}
    df[['is_weekday', 'is_holiday']] = df[['is_weekday', 'is_holiday']].replace(string_dict)

    # fix the number of total docks for a given day
    total_docks = df.groupby(['date']).max().tot_docks.reset_index()
    df = pd.merge(df, total_docks, how = 'left', on = 'date').rename(columns = {'tot_docks_y': 'tot_docks'})
    df.drop('tot_docks_x', 1, inplace=True)

    # engineer new features
    df['flux'] = df.bikes_in - df.bikes_out
    df['pct_avail_bikes'] = df.avail_bikes / df.tot_docks
    df['pct_avail_docks'] = df.avail_docks / df.tot_docks
    df['pct_flux'] = df.flux / df.tot_docks

    #normalize precipitation
    df['precip'] = df.precip / df.precip.max()
    df = df.fillna(method = 'bfill', axis = 0)

    return df

# create flux data table from trips dataframe and rebal dataframe

def split_off_times(df):
    '''
    create 'start/stop_hour' and 'start/stop_date' features
    '''
    df['start_hour'] = df.start_time.dt.hour
    df['stop_hour'] = df.stop_time.dt.hour
    df[['start_date', 'stop_date']] = df[['start_time', 'stop_time']].apply(lambda x: x.dt.floor('d'))
    return df

def create_fluxes(df, id_key, date_key, hour_key, fl_key):
    '''
    create a dataframe of fluxes from grouping by input keys and counting trips

    inputs:
    id_key and hour_key are start_id/stop_id and start_date/end_date
    start is associated with an fl_key = "flux_out",
    and stop is associated with fl_key = "flux_in"
    '''
    use_cols = [id_key, date_key, hour_key, 'duration']

    flux = df.groupby([id_key, date_key, hour_key]).count()
    flux = flux.reset_index()[use_cols]
    col_dict = {'duration': fl_key,
                date_key: 'date', hour_key: 'hour',
                id_key: 'id'}
    return flux.rename(columns = col_dict)


def transform_times(df):
    '''
    calculate approximate pickup and drop-off times for rebalancing trips
    '''
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
    # 'id' 'date' and 'hour'
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


    features = merge_fluxes(merged, rmerged)

    # ------ add more features ---------------------------
    # add station availability data
    avail_db = station_data(year,month)
    features = merge_fluxes(features, avail_db)
    features = features[features.year != 0.]

    return features
