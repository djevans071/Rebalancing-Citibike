#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 21:04:48 2017

@author: psamtik071
"""

from workflow.data import get_data
import pandas as pd

# request zip file and extract contents
zip_URL_base = 'https://s3.amazonaws.com/tripdata/'

# loop through months and years to extract data from data repository
# with get_data in workflow/data.py
def gather_data():
    for year in xrange(2013, 2017):
        for month in xrange(1,13):
            print "{}.{:02}".format(year,month)
            filename =  '{}{:02}-citibike-tripdata.zip'.format(year, month)
            URL = zip_URL_base + filename
            try:
                get_data(filename, URL)
            except:
                print '\t{} does not exist'.format(filename)
                continue


        #data = data.ap(append(get_data(filename, URL))
        #csv_name = 'tripdata/' + filename[:-3] + 'csv'
        #print pd.read_csv(csv_name).columns
        #data.info()
