#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from cbalancer import app
from flask import render_template, request

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import pandas as pd

from model import *

from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.models import FuncTickFormatter, HoverTool

import folium
from datetime import datetime
import pytz
import pdb
# use US/Eastern timezone
est = pytz.timezone('US/Eastern')

username = 'psam071'
host = 'localhost'
dbname = 'citibike'

db = create_engine('postgres://%s%s/%s' % (username,host,dbname))
con = None

con = psycopg2.connect(database = dbname, user = username, host = host)
reg = load_regressor()


# SQL query to get all bike info for 2016
def fetch_query(number):
    query = """
        SELECT a.id, a.date, a.hour, bikes_out, bikes_in, dayofweek, month, is_weekday, is_holiday, tot_docks, avail_bikes, avail_docks, precip, temp
        FROM features_subset a
        LEFT JOIN weather b ON a.date = b.date AND a.hour = b.hour
        LEFT JOIN stations c ON a.id = c.id
        WHERE a.id = {}
            AND tot_docks > 0
            --AND a.date > '2016-03-01'
        --WHERE tot_docks > 0
        ORDER BY a.id, a.date, a.hour;
            """.format(number)

    df=pd.read_sql_query(query,con)
    return df

# get 2015 stations from static pickle file
def get_stations():
    bor_neigh_cols = ['borough', 'neighborhood']
    df = pd.read_pickle('stations.pickle')
    df = df.sort_values(bor_neigh_cols + ['name'])
    return df

# reformat columns of the dataframe from fetch_query (e.g. flux)
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
    return df

# get hourly profile dataframe for any bike for any day
def flux_by_hour(df, cols, dock_id, day = 0, month = 1):
    grps = df.groupby(['id','month','dayofweek', 'hour']).mean().reset_index()
    cond = (grps.dayofweek == day) & (grps.month == month)
    return grps[cond][cols]

# --------------- HOME PAGE ---------------------

@app.route('/index')
@app.route('/')
def index():
    user = {'nickname': 'Dan'}

    return render_template("index.html",
        # neighborhood_list = hood_list,
        name = user)

# ------------- INPUT PAGE -------------------station_number = request.args.get('station-select')

# @app.route('/')
@app.route('/input', methods = ['GET', 'POST'])
def input():

    # drop down menu for station selections
    stations_info = get_stations()

    return render_template('input.html',
        station_df = stations_info)

# ---------------- OUTPUT PAGE --------------------------



def ticker():
    labels = {0:'12 AM', 5:'5 AM', 10:'10 AM',
                15:'3 PM', 20:'8 PM', 24:'12 AM'}
    return labels[tick]

def plotter(df, station_name):
    # plot fluxes
    now = datetime.now(est).hour
    fluxes = df.pct_flux

    # calculate errors, but keep the minimum threshold 0.1
    error = fluxes.std()
    if error < 0.1:
        error = 0.1
    error_plus = fluxes.mean() + error
    error_minus = fluxes.mean() - error

    p1 = figure(plot_width=450, plot_height=350, x_axis_type='datetime')
    p1.quad(top = error_plus, bottom=error_minus, left = now, right = now+1,
            alpha=0.5, line_width=0, color = 'red')
    p1.ray(x=0, y=0, length=24, angle=0,
            color='black')
    p1.ray(x=0, y=error_plus, length=24, angle=0,
            color='black', line_dash = 'dashed')
    p1.ray(x=0, y=error_minus, length=24, angle=0,
            color='black', line_dash = 'dashed')
    p1.line(df['hour'], df['pct_flux'], line_width=4)
    p1.xaxis.axis_label = "Time of Day"
    p1.yaxis.axis_label = "Bikes In per Hour"
    p1.title.text_font_size = "15pt"
    p1.xaxis.axis_label_text_font_size = "13pt"
    p1.yaxis.axis_label_text_font_size = "13pt"
    p1.xaxis.major_label_text_font_size = "12pt"
    p1.yaxis.major_label_text_font_size = "12pt"
    # p1.yaxis.tick_label_text_font_size = "12pt"

    # relabel x-ticks (check ticker function)
    p1.xaxis.formatter = FuncTickFormatter.from_py_func(ticker)
    flux_plot = file_html(p1, CDN, "hourly fluxes")
    return flux_plot


@app.route('/output', methods = ['GET', 'POST'])
def output():
    # pull 'station' from input field and store it
    station_number = request.args.get('station-select')
    # pdb.set_trace()
    station_number = float(station_number)

    #just select the bike_out from the citibike database for the station that the user inputs
    stations_info = get_stations()
    station_name = stations_info[stations_info.id == station_number][['name', 'neighborhood', 'borough']].iloc[0]
    station_loc = list(stations_info[stations_info.id == station_number][['name', 'lat', 'long']].iloc[0])
    # print station_name

    # make station map
    tileset = r'http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'

    station_map = folium.Map(location = station_loc[1:],
        width = 350, height = 350,
        tiles = tileset,
        attr = '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>',
        zoom_start = 15)

    folium.Marker(location = station_loc[1:], icon=folium.Icon(color='red'),
                   popup = station_loc[0]).add_to(station_map)
    station_map.save('cbalancer/templates/station_map.html')

    # get current station data from citibike json feed
    now_station = get_live_station_data(station_number)

    temp = get_live_temp()
    now = datetime.now(est)
    # get data for chosen station
    df = fetch_query(station_number)
    df = new_features(df)
    df = flux_by_hour(df, ['pct_flux','hour'], stations_info, day = now.weekday(), month = now.month)
    fluxes = df.pct_flux
    error = fluxes.std()
    if error < 0.1:
        error = 0.1
    error_plus = fluxes.mean() + error
    error_minus = fluxes.mean() - error

    # make flux plot
    flux_plot = plotter(df, station_name)


    # make recommendation
    station_rec = 0
    # pdb.set_trace()
    if (now_station.statusKey.iloc[0] == 3):
        station_rec = 'Station Offline'
    else:
        try:
            station_rec = round(apply_model(station_number, reg)[0],3)
        except ValueError:
            station_rec = 'Prediction Failed'

    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday',
            'Saturday', 'Sunday']

    return render_template("output.html",
        time = now.time().strftime('%I %p').lstrip('0'),
        dayofweek = days[now.weekday()],
        bike_avail = now_station,
        now_temp = temp,
        rec = station_rec,
        st_info = station_name,
        station_df = stations_info,
        hourly_table = df,
        selected_id = station_number,
        plot = flux_plot,
        error_plus = error_plus, error_minus = error_minus)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/contact')
def contact():
    return render_template('contact.html')
