import cherrypy
from jinja2 import Environment, FileSystemLoader
from postgres.PostgreSQL import PostgreSQL

from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.models import Range1d
from bokeh.layouts import gridplot
from bokeh.plotting import figure, show, output_file

import numpy as np
import pandas as pd

import os, os.path
import random
import string


def get_df(psql, table):
    query = "select column_name from information_schema.columns \
    where table_name = '{}'".format(table)
    psql.execute(query)
    columns = [col[0] for col in psql.cur.fetchall()]
    df = pd.DataFrame(psql.select())
    df.columns = columns
    return df


class Root:
    @cherrypy.expose
    def index(self, table = 'weather', X = 'temp_f_avg', Y = None):
        with PostgreSQL(table = table, database = 'pittsburgh') as psql:
            df = get_df(psql, table)

        if Y is None:
            title = X
            plot = figure(title=X, background_fill_color="#E8DDCB")

            hist, edges = np.histogram(df[X].dropna(), density=True, bins=50)

            plot.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:],
                    fill_color="#036564", line_color="#033649")
        else:
            title = "{} by {}".format(X, Y)
            plot = figure()
            plot.cross(df[X], df[Y])
            

        html = file_html(plot, CDN, "my plot")

        env = Environment(loader=FileSystemLoader('templates'))

        tmpl = env.get_template('index.html')
        print(tmpl)
        return tmpl.render(salutation ='Pittsburgh: ',
                           target     = title, 
                           title      = "Pittsburgh",
                           plot       = html,
                           X    = tuple(df.columns),
                           Y    = tuple(df.columns),
        )


    @cherrypy.expose
    def map(self, webmap = '7694ded02a524c97ae145fa3648081a9'):
        plot = ''.format(webmap)
        
        env = Environment(loader=FileSystemLoader('templates'))

        tmpl = env.get_template('index.html')
        print(tmpl)
        return tmpl.render(salutation='Pittsburgh: ',
                           target='Median Age Map',
                           title = "Median Age",
                           plot = html,
                           sidebar = None,
        )
    

cherrypy.config.update({
    'server.socket_host': '0.0.0.0',
    'server.socket_port': 8080,
})

webapp = Root()
cherrypy.quickstart(webapp, '/')
