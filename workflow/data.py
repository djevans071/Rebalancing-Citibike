import requests, zipfile
import StringIO
import pandas as pd
import os
import pdb
import numpy as np

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


# extract trip data from tripdata/ folder, specifying month and day
def trip_data(year, month):
    '''load trip data for a given year/month'''
    basepath = 'tripdata/'
    csvPath = '{}{:02}-citibike-tripdata.csv'.format(year, month)
    df = pd.read_csv(basepath + csvPath)

    # get rid of strange outliers
    df = df[(df.stop_lat > 40.6) & (df.start_lat > 40.6)]

    # parse times into datetime objects
    df.start_time = pd.to_datetime(df.start_time)
    df.stop_time = pd.to_datetime(df.stop_time)

        # add a column called trip_id within
    #df['trip_id'] = df.index.values
    return df


# extract rebalancing data from trip data
def shift_cond(bike_df):
    """
    Helper function that shifts trips for a given bike to detect
    rebalancing events
    """
    shift_cols = ['stop_id','stop_time', 'stop_long', 'stop_lat', 'stop_name']
    bike_df[shift_cols] = bike_df[shift_cols].shift(1)
    return bike_df[bike_df.start_id != bike_df.stop_id]


def rebal_from_trips(trips):
    """
    Helper function that returns the rebalanced trips from trip data
    """
    trips = trips.sort_values(['bike_id', 'start_time'])
    rebal_df = trips.groupby('bike_id').apply(shift_cond).dropna()
    rebal_df.index = rebal_df.index.droplevel('bike_id')
    #pdb.set_trace()
    return rebal_df

def rebal_data(year, month):
    '''load rebal data for a given year/month'''
    basepath = 'rebals/'
    csvPath = '{}{:02}-rebal-data.csv'.format(year, month)
    df = pd.read_csv(basepath + csvPath)

    # get rid of strange outliers
    df = df[df.stop_lat > 40.6]

    # parse times into datetime objects
    df.start_time = pd.to_datetime(df.start_time)
    df.stop_time = pd.to_datetime(df.stop_time)

    # add a column called trip_id within
    #df['trip_id'] = df.index.values
    return df

# grab station names and locations from trip file
def stations_from_trips(df):
    '''
    create a table of unique stations from the trips dataframe
    '''
    stations = df.groupby('start_id')['start_name', 'start_lat',
       'start_long'].aggregate(lambda x: x.value_counts().index[0])
    stations.columns = ['name','lat','long']
    stations.index.name = 'id'
    stations.sort_index(inplace = True)
    stations.to_csv('stations.csv')
    return stations

# grab station data from stationdata/ and format to merge with features data
def station_data(year, month):
    '''load station data for a given year/month'''
    basepath = 'stationdata/'
    csvPath = '{}{:02}-bikeshare_nyc_raw.csv'.format(year,month)

    cols = ['dock_id', 'avail_bikes', 'avail_docks', '_lat', '_long',
            'dock_name', 'date', 'hour', 'minute', 'pm', 'tot_docks']

    df = pd.read_csv(basepath + csvPath,
                     delimiter = '\t', error_bad_lines = False)

    # convert columns to numeric datatypes
    #cols = ['dock_id', 'avail_bikes', 'avail_docks', '_lat', '_long']
    df[cols[:5]] = df[cols[:5]].apply(pd.to_numeric, errors='coerce')
    #df = df.iloc[:,:13]
    # df = df.dropna()
    # pdb.set_trace

    # get rid of strange outliers
    df = df[df._lat > 40.6]
    df = df[df.tot_docks > 0]
    df = df.rename(columns = {'dock_id': 'id'})

    # parse and modify time columns
    formatting = '%y-%m-%d'
    df['date'] = pd.to_datetime(df.date,format = formatting)
    df.loc[(df.hour == 12) & (df.pm == 0), 'hour'] = 0
    df.loc[(df.hour == 12) & (df.pm == 1), 'hour'] = 0
    df.hour = np.where(df.pm == 0, df.hour, df.hour+12)


    # aggregate by hour
    aggregator = {'avail_bikes':'mean',
              'avail_docks':'mean', 'tot_docks':'max'}
    df = df.groupby(['id', 'date', 'hour']).agg(aggregator)
    df = df.applymap(round).reset_index()

    return df
