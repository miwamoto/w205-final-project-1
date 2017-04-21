import cherrypy
from jinja2 import Environment, FileSystemLoader
from postgres.PostgreSQL import PostgreSQL

from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.plotting import figure

import numpy as np
import pandas as pd

def fetch_tables(search = None, limit = 10):
    with PostgreSQL(database = 'pittsburgh') as psql:
        query = "select DISTINCT table_name from information_schema.columns"
        if search is not None:
            query += " where table_name like '%{}%'".format(search)
        if limit is not None:
            query += " limit {}".format(limit)

        psql.execute(query)
        return [table[0] for table in psql.cur.fetchall()]



def get_df(psql, table):
    query = "select column_name from information_schema.columns \
    where table_name = '{}'".format(table)
    psql.execute(query)
    columns = [col[0] for col in psql.cur.fetchall()]
    df = pd.DataFrame(psql.select())
    df.columns = columns
    return df


def isNumeric(dtype):
    print(dtype)
    if str(dtype) in ('int64', 'float64'):
        return True
    else:
        return False

class Root:
    @cherrypy.expose
    def index(self, table = 'weather', X = None, Y = None, search = None, limit = 10):
        with PostgreSQL(table = table, database = 'pittsburgh') as psql:
            df = get_df(psql, table)

            tables = fetch_tables(search = search, limit = limit)

        if X is None and Y is None:
            html = ''
            title = ''
            
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
            

        env = Environment(loader=FileSystemLoader('templates'))
        cols = tuple(df.columns)
        dtypes = tuple(df.dtypes)
        numeric = [col for col, dtype in zip(cols, dtypes) if isNumeric(dtype)]
        categorical = [col for col, dtype in zip(cols, dtypes) if not isNumeric(dtype)]

        tmpl = env.get_template('index.html')
        print(tmpl)
        return tmpl.render(salutation ='Pittsburgh: ',
                           target     = title, 
                           title      = "Pittsburgh",
                           plot       = html,
                           X    = numeric,
                           Y    = numeric,
                           groupby = categorical,
                           tables = tables,
        )


    @cherrypy.expose
    def map(self, webmap = '7694ded02a524c97ae145fa3648081a9'):
        webmap = ''.format(webmap)
        
        env = Environment(loader=FileSystemLoader('templates'))

        tmpl = env.get_template('map.html')
        print(tmpl)
        return tmpl.render(salutation='Pittsburgh: ',
                           target='Median Age Map',
                           title = "Median Age",
                           webmap = webmap,
                           sidebar = None,
        )
    

cherrypy.config.update({
    'server.socket_host': '0.0.0.0',
    'server.socket_port': 8080,
})

webapp = Root()
cherrypy.quickstart(webapp, '/')
