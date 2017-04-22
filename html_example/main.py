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


def get_col_names(table):
    with PostgreSQL(table = table, database = 'pittsburgh') as psql:
        query = "select column_name, data_type from information_schema.columns \
            where table_name = '{}'".format(table)
        psql.execute(query)
        res = psql.cur.fetchall()
    columns = [col[0] for col in res]
    dtypes = [col[1] for col in res]
    return columns, dtypes


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
    def index(self, table = 'weather', X = None, Y = None, search = None, limit = 10, size = 1, table_limit = 5000):
        env = Environment(loader=FileSystemLoader('templates'))
        tmpl = env.get_template('index.html')
        tables = fetch_tables(search, table_limit)
        colnames, dtypes = get_col_names(table)

        if table is not None:
            df = get_df(cols = [x for x in (X, Y, ) if x is not None], table = table, limit = limit)

        X = X if X in colnames else None
        Y = Y if Y in colnames else None

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
        # cols = tuple(df.columns)
        cols = colnames
        # dtypes = tuple(df.dtypes)
        numeric = [col for col, dtype in zip(cols, dtypes) if isNumeric(dtype)]
        categorical = [col for col, dtype in zip(cols, dtypes) if not isNumeric(dtype)]

        tmpl = env.get_template('index.html')
        del df

        return tmpl.render(salutation ='Pittsburgh: ',
                        target     = title, 
                        title      = "Pittsburgh",
                        plot       = html,
                        X    = numeric,
                        Y    = numeric,
                        groupby = categorical,
                        tables = tables,
        )

        # except Exception as e:
        #     print(e)
        #     tmpl = env.get_template('index.html')
        #     return tmpl.render(salutation ='Pittsburgh: ',
        #                     target     = "Try Again", 
        #                     title      = "Pittsburgh",
        #                     plot       = "Could not complete your request",
        #     )


    def POST(self, table):
        return "X"


    @cherrypy.expose
    def map(self, webmap = '7694ded02a524c97ae145fa3648081a9'):
        webmap = ''.format(webmap)
        
        env = Environment(loader=FileSystemLoader('templates'))

        tmpl = env.get_template('map.html')

        from bokeh.events import ButtonClick
        from bokeh.models import Button, CustomJS

        button = Button()

        button.js_on_event(ButtonClick, CustomJS(code='console.log("JS:Click")'))
        return tmpl.render(salutation='Pittsburgh: ',
                           target='Median Age Map',
                           title = "Median Age",
                           webmap = webmap,
        )
    

cherrypy.config.update({
    'server.socket_host': '0.0.0.0',
    'server.socket_port': 8080,
})

webapp = Root()
cherrypy.quickstart(webapp, '/')
