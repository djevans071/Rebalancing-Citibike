from flask import render_template, request
from websitetools import app

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

@app.route('/index')
def index():
    user = {'nickname': 'Dan'}
    return render_template("index.html",
        title = 'Home',
        user = user, num = 5)

@app.route('/')
@app.route('/input')
def cesareans_input():
    return render_template('input.html')

@app.route('/output')
def cesareans_output():
    # pull 'birth_month' from input field and store it
    # patient = request.args.get('birth_month')
    station_number = request.args.get('station')
    print station_number
    #just select the Cesareans  from the birth dtabase for the month that the user inputs
    query = """
            SELECT hour, sum(bikes_out) as bikes_out
            FROM features
            WHERE id = {}
            GROUP BY hour
            ORDER BY hour;
            """.format(station_number)
    print query
    results=pd.read_sql_query(query,con)
    print results
    rows = []
    for i in range(0,results.shape[0]):
        rows.append(dict(
        hour=results.iloc[i]['hour'],
        bikes_out=results.iloc[i]['bikes_out']))
        #the_result = ModelIt(patient,births)
    return render_template("output.html", rows = rows)#, the_result = the_result)

@app.route('/db')
def bikes_page():
    query = '''
            SELECT hour, sum(bikes_out) as bikes_out
            FROM features
            WHERE id = 72
            GROUP BY hour
            ORDER BY hour;
        '''
    results = pd.read_sql_query(query, con)
    rows = ''
    for i in range(24):
        rows += '{} | {}'.format(results.iloc[i,0], results.iloc[i,1])
        rows += '<br>'
    return rows
    # for i in range(10):
    #     births += results.iloc[i]['birth_month']
    #     births += '<br>'
    # return births

@app.route('/db_fancy')
def cesarean_page_fancy():
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
