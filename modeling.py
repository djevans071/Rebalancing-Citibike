#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 07:32:49 2017

@author: psamtik071
"""

# routines for further feature creation for modeling purposes

from matplotlib import pyplot as plt
import seaborn as sns
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
query_stations2015 = """
    SELECT DISTINCT a.id
    FROM features a
    LEFT JOIN stations b ON a.id = b.id
    WHERE a.date < '2016-03-01' AND tot_docks > 0
    ORDER BY a.id;
"""

stations_2015 = pd.read_sql_query(query_stations2015, con)

def cleanup(year):
    df = pd.read_sql_query(bulk_query(year),con)
    df = strip_unused_stations(df, stations_2015.id)
    df = new_features(df)
    df.date = pd.to_datetime(df.date)

    data_cols = ['hour', 'month', 'is_weekday', 'is_holiday', 'precip', 'temp']
    hist_cols = ['mean_flux', 'yest_flux', 'last_week_flux']
    return make_categorical(df, ['id'] + data_cols + hist_cols)

index_col = ['date']
data_cols = ['id', 'hour', 'month', 'is_weekday', 'is_holiday',
             'precip', 'temp']
hist_cols = ['mean_flux', 'yest_flux', 'last_week_flux']

feature_list = data_cols + hist_cols

df2015 = cleanup(2015)[feature_list + ['flux_type']]
df2016 = cleanup(2016)[feature_list + ['flux_type']]

# ------------ MODELING -------------------
print "Training classifier..."
# train model
from sklearn.ensemble import RandomForestClassifier

# data = df[data_cols + hist_cols].sort_index()
# target = df.flux_type

# X_train, X_test, y_train, y_test = train_test_split(data, target,
#   train_size = 0.75, test_size = 0.25)

X_train = df2015[data_cols + hist_cols]
y_train = df2015.flux_type

X_test = df2016[data_cols  + hist_cols]
y_test = df2016.flux_type

clf = RandomForestClassifier(max_features=0.95,
                                  min_samples_leaf=6,
                                  min_samples_split=3,
                                  n_estimators=100, n_jobs = -1)

clf.fit(X_train, y_train)
pred = clf.predict(X_test)

print "Saving Data..."
# ------------- SAVE DATA TO PICKLE --------------
import pickle

CLF_PICKLE_FILENAME = "classifier.pkl"
DATASET_PICKLE_FILENAME = "dataset.pkl"
FEATURE_LIST_FILENAME = "feature_list.pkl"

def dump_classifier_and_data(clf, dataset, feature_list):
    with open(CLF_PICKLE_FILENAME, "w") as clf_outfile:
        pickle.dump(clf, clf_outfile)
    with open(DATASET_PICKLE_FILENAME, "w") as dataset_outfile:
        pickle.dump(dataset, dataset_outfile)
    with open(FEATURE_LIST_FILENAME, "w") as featurelist_outfile:
        pickle.dump(feature_list, featurelist_outfile)

def load_classifier_and_data():
    with open(CLF_PICKLE_FILENAME, "r") as clf_infile:
        clf = pickle.load(clf_infile)
    with open(DATASET_PICKLE_FILENAME, "r") as dataset_infile:
        dataset = pickle.load(dataset_infile)
    with open(FEATURE_LIST_FILENAME, "r") as featurelist_infile:
        feature_list = pickle.load(featurelist_infile)
    return clf, dataset, feature_list

dump_classifier_and_data(clf, df2015, feature_list)
