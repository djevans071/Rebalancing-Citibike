#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 22:15:40 2017

@author: psamtik071
"""

#functions for feature-engineering

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

# create a daily avg flux column and shift it to get yesterday's flux for a given date.
# also with weekly fluxes
def make_lagged_fluxes(df):
    mean_daily_flux = df.groupby('date').mean().flux
    mean_yesterday_flux = mean_daily_flux.shift(1).reset_index()
    mean_lastweek_flux = mean_daily_flux.shift(7).reset_index()

    mean_daily_flux = mean_daily_flux.reset_index().rename(columns = {'flux': 'mean_flux'})
    mean_yesterday_flux = mean_yesterday_flux.rename(columns = {'flux': 'yest_flux'})
    mean_lastweek_flux = mean_lastweek_flux.rename(columns = {'flux': 'last_week_flux'})

    dfs = [df, mean_daily_flux, mean_yesterday_flux, mean_lastweek_flux]
    return reduce(merge_by_date, dfs)

def new_features(df):
    # df['date'] = pd.to_datetime(df.date)
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
    # df['pct_bikes_in'] = df.bikes_in / df.tot_docks
    # df['pct_bikes_out'] = df.bikes_out / df.tot_docks
    df['pct_avail_bikes'] = df.avail_bikes / df.tot_docks
    df['pct_avail_docks'] = df.avail_docks / df.tot_docks
    df['pct_flux'] = df.flux / df.tot_docks
    #df['pct_rebal_flux'] = df.rebal_net_flux / df.tot_docks


    #normalize precipitation
    df['precip'] = df.precip / df.precip.max()
    df = df.fillna(method = 'bfill', axis = 0)


    # get lagged features
    # df_with_lags = make_lagged_fluxes(df).dropna()

    # hist_cols = ['mean_flux', 'yest_flux', 'last_week_flux']
#     for col in hist_cols:
#         df_with_lags[col] = df_with_lags[col].apply(flux_conditions).astype('category')
#     df_with_lags = df_with_lags.dropna()
    # features_to_clear = ['bikes_out', 'bikes_in','rebal_net_flux',
        # 'tot_docks', 'avail_bikes', 'avail_docks', 'flux']

    return df
