#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 07:32:49 2017

@author: psamtik071
"""

# routines for further feature creation for modeling purposes

from matplotlib import pyplot as plt
#import seaborn as sns
from workflow.data import *
from workflow.features import *
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
#from sqlalchemy_utils import database_exists, create_database
import psycopg2

# connect to SQL database
username = 'psam071'
host = 'localhost'
dbname = 'citibike'

db = create_engine('postgres://%s%s/%s' % (username,host,dbname))
con = None

con = psycopg2.connect(database = dbname, user = username, host = host)

print "Querying Database..."
#get stations for 2015 to work on
# query_stations2015 = """
#     SELECT DISTINCT a.id
#     FROM features a
#     LEFT JOIN stations b ON a.id = b.id
#     WHERE a.date = '2015-03-01' AND tot_docks > 0
#     ORDER BY a.id;
# """

stations_2015 = pd.read_pickle('websitetools/stations.pickle')

def cleanup(year):
    # clean-up dataset further and introduce new features from new_features module
    df = pd.read_sql_query(bulk_query(year),con)
    df = strip_unused_stations(df, stations_2015.id.unique())
    df = new_features(df)
    df.date = pd.to_datetime(df.date)
    return df

index_col = ['date']
data_cols = ['id', 'long', 'lat', 'hour', 'dayofweek',
             'month', 'is_weekday', 'is_holiday',
             'precip', 'temp', 'pct_avail_bikes', 'pct_avail_docks']

df2015 = cleanup(2015)
df2016 = cleanup(2016)

# ------------ MODELING -------------------
print "Training predictor..."
# train model
from sklearn.ensemble import RandomForestRegressor

# data = df[data_cols + hist_cols].sort_index()
# target = df.flux_type

# X_train, X_test, y_train, y_test = train_test_split(data, target,
#   train_size = 0.75, test_size = 0.25)
target_label = 'pct_flux'

X_train = df2015[data_cols]
y_train = df2015[target_label]

X_test = df2016[data_cols]
y_test = df2016[target_label]

reg = RandomForestRegressor(min_samples_leaf=16, min_samples_split=6,
                            max_features = 0.95, n_jobs=-1)
reg.fit(X_train, y_train)
pred = reg.predict(X_test)

print "Saving Data..."
# ------------- SAVE DATA TO PICKLE --------------
import pickle

PICKLE_FILENAME = "regressor.pkl"
DATASET_PICKLE_FILENAME = "dataset.pkl"
FEATURE_LIST_FILENAME = "feature_list.pkl"

def dump_model_and_data(clf, dataset, feature_list):
    with open(PICKLE_FILENAME, "w") as reg_outfile:
        pickle.dump(reg, reg_outfile)
    with open(DATASET_PICKLE_FILENAME, "w") as dataset_outfile:
        pickle.dump(dataset, dataset_outfile)
    with open(FEATURE_LIST_FILENAME, "w") as featurelist_outfile:
        pickle.dump(feature_list, featurelist_outfile)

def load_model_and_data():
    with open(PICKLE_FILENAME, "r") as reg_infile:
        reg = pickle.load(reg_infile)
    with open(DATASET_PICKLE_FILENAME, "r") as dataset_infile:
        dataset = pickle.load(dataset_infile)
    with open(FEATURE_LIST_FILENAME, "r") as featurelist_infile:
        feature_list = pickle.load(featurelist_infile)
    return reg, dataset, feature_list

dump_model_and_data(reg, df2015, data_cols)
