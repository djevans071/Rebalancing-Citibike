import requests, zipfile
import StringIO
import pandas as pd
import os
import pdb

def get_data(name, url):
    """ Download and cache Citibike data
    """
    out_path = 'tripdata/'
    csv_path = out_path + name[:-3] + 'csv'
    if os.path.exists(csv_path):
        print "\t{} already downloaded".format(csv_path)
    else:
        # request zipfile and extract
        r = requests.get(url, timeout=5)
        z = zipfile.ZipFile(StringIO.StringIO(r.content))
        orig_name = z.namelist()[0]
        z.extract(orig_name, out_path)
        z.close()

        #rename extracted file
        os.rename(out_path + orig_name, csv_path)
        print '\tzip file removed'
        print '\t{} saved'.format(csv_path)


def trip_data(year, month):
    '''make a df of trip data for a given year/month'''
    basepath = 'tripdata/'
    csvPath = '{}{:02}-citibike-tripdata.csv'.format(year, month)
    df = pd.read_csv(basepath + csvPath, parse_dates = ['Start Time', 'Stop Time'])

    # rename columns
    new_cols = {'Trip Duration':'duration',
                'Start Time': 'start_time',
                'Stop Time':'stop_time',
                'Start Station ID':'start_id',
                'Start Station Name':'start_name',
                'Start Station Latitude':'start_lat',
                'Start Station Longitude':'start_long',
                'End Station ID':'stop_id',
                'End Station Name':'stop_name',
                'End Station Latitude':'stop_lat',
                'End Station Longitude':'stop_long',
                'Bike ID':'bike_id',
                'User Type':'user_type',
                'Birth Year':'birth_year',
                'Gender':'gender'}

    df = df.rename(columns = new_cols)
    # remove outlier (lat 0, long 0)
    df = df[df.stop_lat != 0]

        # add a column called trip_id within
    df['trip_id'] = df.index.values
    return df

def stations_from_trips(df):
    stations = df.groupby('start_id')['start_name', 'start_lat',
           'start_long'].aggregate(lambda x: x.value_counts().index[0])
    stations.columns = ['name','lat','long']
    stations.index.name = 'id'
    stations.sort_index(inplace = True)
    stations.to_csv('stations.csv')
    return stations

