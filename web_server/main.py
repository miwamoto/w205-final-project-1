#!/home/ec2-user/anaconda3/bin/python
import cherrypy
from jinja2 import Environment, FileSystemLoader
from postgres.PostgreSQL import PostgreSQL

from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.plotting import figure

import numpy as np
import pandas as pd
import os
import json
from sqlalchemy import create_engine

clean_engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5432/pittsburgh_clean",
    isolation_level="READ UNCOMMITTED"
)

database = 'pittsburgh_clean'


table_names = {
    'allegheny_county_anxiety_medication_0': 'Anxiety Medication 1',
    'allegheny_county_anxiety_medication_1': 'Anxiety Medication 2',
    'allegheny_county_boundary_3': 'County Boundary',
    'allegheny_county_crash_data_15': 'Crash Data',
    'allegheny_county_depression_medication_0': 'Depression Medication 1',
    'allegheny_county_depression_medication_1': 'Depression Medication 2',
    'allegheny_county_diabetes_hospitalization_0': 'Diabetes Hospitalization 0',
    'allegheny_county_diabetes_hospitalization_1': 'Diabetes Hospitalization 1',
    'allegheny_county_fatal_accidental_overdoses_1': 'Fatal Accidental Overdoses 1',
    'allegheny_county_fatal_accidental_overdoses_2': 'Fatal Accidental Overdoses 2',
    'allegheny_county_hypertension_hospitalization_0': 'Hypertension Hospitalization 0',
    'allegheny_county_hypertension_hospitalization_1': 'Hypertension Hospitalization 1',
    'allegheny_county_land_cover_areas_3': 'Land Cover Areas 3',
    'allegheny_county_median_age_at_death_0': 'Median Age At Death 0',
    'allegheny_county_median_age_at_death_1': 'Median Age At Death 1',
    'allegheny_county_municipal_boundaries_3': 'Municipal Boundaries 3',
    'allegheny_county_obesity_rates_1': 'Obesity Rates 1',
    'allegheny_county_obesity_rates_2': 'Obesity Rates 2',
    'allegheny_county_poor_housing_conditions_0': 'Poor Housing Conditions 0',
    'allegheny_county_poor_housing_conditions_1': 'Poor Housing Conditions 1',
    'allegheny_county_primary_care_access_0': 'Primary Care Access 0',
    'allegheny_county_primary_care_access_1': 'Primary Care Access 1',
    'allegheny_county_smoking_rates_0': 'Smoking Rates 0',
    'allegheny_county_smoking_rates_1': 'Smoking Rates 1',
    'allegheny_county_zip_code_boundaries_3': 'Zip Code Boundaries 3',
    'crime_forecasts': 'Crime Forecasts',
    'diabetes_0': 'Diabetes 0',
    'diabetes_1': 'Diabetes 1',
    'geocoders_0': 'Geocoders 0',
    'hyperlipidemia_0': 'Hyperlipidemia 0',
    'hyperlipidemia_1': 'Hyperlipidemia 1',
    'hypertension_0': 'Hypertension 0',
    'hypertension_1': 'Hypertension 1',
    'metatable': 'Metatable',
    'neighborhoods_with_snap_data_3d3a9_3': 'Neighborhoods With Snap Data',
    'non_traffic_citations_0': 'Non Traffic citations 0',
    'pgh_snap_13': 'Snap Data 13',
    'pgh_snap_14': 'Snap Data 14',
    'pgh_snap_16': 'Snap Data 16',
    'pgh_snap_17': 'Snap Data 17',
    'pgh_snap_18': 'Snap Data 18',
    'pgh_snap_19': 'Snap Data 19',
    'pgh_snap_20': 'Snap Data 20',
    'pittsburgh_police_arrest_data_0': 'Pittsburgh Police Arrest Data',
    'pittsburgh_police_sectors_5f5e5_3': 'Pittsburgh Police Sectors',
    'pittsburgh_police_zones_4763f_3': 'Pittsburgh Police Zones',
    'police_incident_blotter_30_day_0': 'Police Incident Blotter 30 Day',
    'police_incident_blotter_archive_0': 'Police Incident Blotter Archive 0',
    'police_incident_blotter_archive_2': 'Police Incident Blotter Archive 2',
    'poverty': 'Poverty',
    'tmp': '',
    'weather': 'Weather',
    'weather_forecasts': 'Weather Forecasts'
}

def fetch_tables(search = None, limit = 10):
    with PostgreSQL(database = database) as psql:
        query = "select DISTINCT table_name from information_schema.columns where table_schema = 'public'"
        if search is not None:
            query += " and table_name like '%{}%'".format(search)
        if limit is not None:
            query += " limit {}".format(limit)

        psql.execute(query)
        return [table[0] for table in psql.cur.fetchall()]


def plot(table = None, X = None, Y = None, groupby = None, limit = 5000):
    print('##############################')
    print('##############################')
    print('##############################')
    print('##############################')
    print('##############################')
    colnames, _ = get_col_names(table)
    X = X if X in colnames else None
    Y = Y if Y in colnames else None

    if table is None:
        return 'Please select a table'
    else:
        print(X, Y)
        print('#####')
        df = get_df(cols = [x for x in (X, Y, ) if x is not None], table = table, limit = limit)
        print('$$$$$')
        print(X, Y)
        print(df.head())


    if X is None and Y is None:
        return "Please select a value to plot"

    elif Y is None or X is None:
        if X is None:
            X = Y
        title = X
        plot = figure(title=X, background_fill_color="#E8DDCB")

        hist, edges = np.histogram(df[X].dropna(), density=True, bins=50)

        plot.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:],
                fill_color="#036564", line_color="#033649")
        html = file_html(plot, CDN, "my plot")
    else:
        title = "{} by {}".format(X, Y)
        plot = figure()
        plot.cross(df[X], df[Y])
        html = file_html(plot, CDN, "my plot")

    return html


def get_col_names(table):
    with PostgreSQL(table = table, database = database) as psql:
        query = "select column_name, data_type from information_schema.columns \
            where table_name = '{}'".format(table)
        psql.execute(query)
        res = psql.cur.fetchall()
    columns = [col[0] for col in res]
    dtypes = [col[1] for col in res]
    return columns, dtypes

def split_col_types(table):
    colnames, dtypes = get_col_names(table)
    cols = colnames
    numeric = [col for col, dtype in zip(cols, dtypes) if isNumeric(dtype)]
    categorical = [col for col, dtype in zip(cols, dtypes) if not isNumeric(dtype)]
    return numeric, categorical


def get_df(table, cols = None, limit = None):
    print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
    print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
    print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
    print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
    colnames, dtypes = get_col_names(table)
    if cols is None:
        columns = "*"
    else:
        columns = ','.join(cols)

    if limit is not None and int(limit) > 0:
        predicates = ' limit {}'.format(limit)
    else:
        predicates = ''

    df = pd.read_sql(table, clean_engine, columns = cols)
    # with PostgreSQL(database = database, table = table) as psql:
    #     # query = "select {} from {}".format(columns, table, )
    #     # psql.execute(query)
    #     # columns = [col[0] for col in ]
    #     df = pd.DataFrame(psql.select(table = table, cols = cols, predicates = predicates))
    print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
    print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
    print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
    print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
    print(cols)
    print(df.columns)
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

    # if cols is not None:
    #     df.columns = list(cols)
    # df = df[list(cols)]
        # df.columns = columns
    return df


def isNumeric(dtype):
    if str(dtype) in ('int64', 'float64', 'integer', 'double precision', 'bigint'):
        return True
    else:
        return False


class Root:
    @cherrypy.expose
    def index(self, table = 'weather', X = None, Y = None, search = None, limit = 5000, size = 1, table_limit = 10):
        env = Environment(loader=FileSystemLoader('templates'))
        tmpl = env.get_template('index.html')
        tables = fetch_tables(search, table_limit)

        colnames, dtypes = get_col_names(table)
        numeric, categorical = split_col_types(table)

        return tmpl.render(salutation ='Pittsburgh: ',
                        target     = '', 
                        title      = "Pittsburgh",
                        # plot       = '',
                        X    = numeric,
                        Y    = numeric,
                        # groupby = categorical,
                        tables = tables,
        )


    @cherrypy.expose
    def map(self, webmap = 'crime_forecast'):
        maps = ['Crime Forecast', 'Arrest Data', ]
        webmaps = [
                   'crime_forecast',
                   'arrest',
        ]
        try:
            title = maps[webmaps.index(webmap)]
        except:
            title = ''

        env = Environment(loader=FileSystemLoader('templates'))
        print(webmap)
        print(webmap)
        print(webmap)
        print(webmap)
        print(webmap)
        print(webmap)
        print(webmap)
        print(webmap)
        print(webmap)
        print(webmap)
        if webmap == 'arrest':
            tmpl = env.get_template('arrest_map.html')
        elif webmap == 'crime_forecast':
            tmpl = env.get_template('forecast_map.html')
        else:
            tmpl = env.get_template('map.html')

        return tmpl.render(salutation='Pittsburgh: ',
                           target = title,
                           title  = title,
                           tables = zip(maps, webmaps),
        )
    
class TableFetcher(object):
    @cherrypy.tools.accept(media='text/plain')
    def GET(self, search = None, limit = 10):
        tables = fetch_tables(search, limit)
        env = Environment(loader=FileSystemLoader('templates'))
        tmpl = env.get_template('links.html')

        return tmpl.render(
                        iterable = tables,
                        section = 'tables',
        )


class Plotter(object):
    @cherrypy.tools.accept(media='text/plain')
    def GET(self, table = 'weather', X = None, Y = None, limit = 5000, groupby = None):
        print(X, Y)
        html = plot(table, X, Y, limit, groupby)
        return html

class GetCols(object):
    @cherrypy.tools.accept(media='text/plain')
    def GET(self, table = 'weather' ):
        numeric, categorical = split_col_types(table)

        env = Environment(loader=FileSystemLoader('templates'))
        tmpl = env.get_template('numeric.html')
        X = tmpl.render(
                        iterable    = numeric,
                        section = "X"
        )
        Y = tmpl.render(
                        iterable    = numeric,
                        section = "Y"
        )

        return json.dumps({'X': X, 'Y': Y})


if __name__ == '__main__':
    conf = {
        '/': {
            'tools.sessions.on': True,
            'tools.staticdir.root': os.path.abspath(os.getcwd()),
        },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public'
        }
    }

    APIS = ('plotter', 'getcols', 'generator')
    for API in APIS:
        conf['/' + API] = {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        }

    cherrypy.config.update({
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8080,
    })

    webapp = Root()
    webapp.generator = TableFetcher()
    webapp.generator.exposed = True
    webapp.plotter = Plotter()
    webapp.plotter.exposed = True
    webapp.getcols = GetCols()
    webapp.getcols.exposed = True

    cherrypy.quickstart(webapp, '/', conf)
