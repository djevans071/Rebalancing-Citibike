from flask import render_template, request
from cbalancer import app

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2

from a_model import ModelIt

username = 'psam071'
host = 'localhost'
dbname = 'citibike'

db = create_engine('postgres://%s%s/%s' % (username,host,dbname))
con = None

con = psycopg2.connect(database = dbname, user = username, host = host)


def get_stations():
    bor_neigh_cols = ['borough', 'neighborhood']
    df = pd.read_pickle('stations.pickle')
    df = df.sort_values(bor_neigh_cols + ['name'])
    return df

# --------------- HOME PAGE ---------------------

@app.route('/index')
@app.route('/index.html')
def index():
    user = {'nickname': 'Dan'}

    return render_template("index.html",
        # neighborhood_list = hood_list,
        station_list = name_list)

# ------------- INPUT PAGE -------------------

@app.route('/')
@app.route('/input')
def input():

    # drop down menu for station selections
    stations_info = get_stations()

    return render_template('input.html',
        station_df = stations_info)

# ---------------- OUTPUT PAGE --------------------------

@app.route('/output', methods = ['GET', 'POST'])
def output():
    # pull 'station' from input field and store it
    station_number = request.form.get('station-select')
    print station_number
    #just select the bike_out from the citibike database for the station that the user unputs

    stations_info = get_stations()
    # print stations_info.head()

    query = """
            SELECT hour, avg(bikes_out) as bikes_out,
                avg(bikes_in) as bikes_in
            FROM features
            WHERE id = {}
            GROUP BY hour
            ORDER BY hour;
            """.format(station_number)
    results=pd.read_sql_query(query,con)

    # render table in output html
    # rows = []
    # for i in range(0,results.shape[0]):
    #     rows.append(dict(
    #         hour=results.iloc[i]['hour'].round(1),
    #         bikes_out=results.iloc[i]['bikes_out'].round(3),
    #         bikes_in = results.iloc[i]['bikes_in'].round(3)))
        #the_result = ModelIt(patient,births)
    return render_template("output.html",
        stations_df = stations_info,
        hourly_table = results)#, the_result = the_result)







# ---------- sample db pages -------------------

@app.route('/db')
def bikes_page():
    query = '''
            SELECT hour, sum(bikes_out) as bikes_out,
                        sum(bikes_in) as bikes_in
            FROM features
            WHERE id = 72
            GROUP BY hour
            ORDER BY hour;
        '''
    results = pd.read_sql_query(query, con)
    rows = ''
    for i in range(24):
        rows += '{} | {} | {}'.format(results.hour[i], results.bikes_out[i], results.bikes_in[i])
        rows += '<br>'
    return rows
    # for i in range(10):
    #     births += results.iloc[i]['birth_month']
    #     births += '<br>'
    # return births

@app.route('/db_fancy')
def bikes_page_fancy():
    query = '''
            SELECT hour, sum(bikes_out) as bikes_out
            FROM features
            WHERE id = 72
            GROUP BY hour
            ORDER BY hour;
        '''

    results = pd.read_sql_query(query,con)
    bikes = []
    for i in xrange(0, results.shape[0]):
        bikes.append(dict(hour = results.iloc[i]['hour'],
        bikes_out = results.iloc[i]['bikes_out']))

    return render_template('bikes_table.html', bikes = bikes)
