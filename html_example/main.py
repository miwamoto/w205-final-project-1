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

def fetch_tables(search = None, limit = 10):
    with PostgreSQL(database = 'pittsburgh') as psql:
        query = "select DISTINCT table_name from information_schema.columns where table_schema = 'public'"
        if search is not None:
            query += " and table_name like '%{}%'".format(search)
        if limit is not None:
            query += " limit {}".format(limit)

        psql.execute(query)
        return [table[0] for table in psql.cur.fetchall()]


def plot(table = None, X = None, Y = None, groupby = None, limit = 5000):
    colnames, _ = get_col_names(table)
    X = X if X in colnames else None
    Y = Y if Y in colnames else None

    if table is not None:
        df = get_df(cols = [x for x in (X, Y, ) if x is not None], table = table, limit = limit)
    else:
        return 'Please select a table'

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
    with PostgreSQL(table = table, database = 'pittsburgh') as psql:
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
    colnames, dtypes = get_col_names(table)
    if cols is None:
        columns = "*"
    else:
        columns = ','.join(cols)

    if limit is not None and int(limit) > 0:
        predicates = ' limit {}'.format(limit)
    else:
        predicates = ''

    with PostgreSQL(database = 'pittsburgh', table = table) as psql:
        # query = "select {} from {}".format(columns, table, )
        # psql.execute(query)
        # columns = [col[0] for col in ]
        df = pd.DataFrame(psql.select(table = table, cols = cols, predicates = predicates))
        if cols is not None:
            df.columns = list(cols)
    # df = df[list(cols)]
        # df.columns = columns
    return df


def isNumeric(dtype):
    if str(dtype) in ('int64', 'float64', 'integer', 'double precision'):
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
    def map(self, webmap = '7694ded02a524c97ae145fa3648081a9'):
        webmap = ''.format(webmap)
        env = Environment(loader=FileSystemLoader('templates'))
        tmpl = env.get_template('map.html')
        return tmpl.render(salutation='Pittsburgh: ',
                           target='Median Age Map',
                           title = "Median Age",
                           webmap = webmap,
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
