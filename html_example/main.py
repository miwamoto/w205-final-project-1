import cherrypy
from jinja2 import Environment, FileSystemLoader
from postgres.PostgreSQL import PostgreSQL

from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.models import Range1d

import numpy as np
import pandas as pd

import os, os.path
import random
import string


class Root:
    @cherrypy.expose
    def index(self):
        with PostgreSQL(table = 'weather', database = 'pittsburgh') as psql:
            psql.execute("select column_name from information_schema.columns \
            where table_name = 'weather'")
            columns = [col[0] for col in psql.cur.fetchall()]
            weather = pd.DataFrame(psql.select())
            weather.columns = columns


        plot = figure()
        plot.cross(weather['year'], weather['temp_f_high'])
        # plot.line(range(0,10), range(0,10))
        # my_rand2 = np.random.random_sample((2,100)) * 10
        # plot.cross(*my_rand2)

        html = file_html(plot, CDN, "my plot")

        env = Environment(loader=FileSystemLoader('templates'))

        tmpl = env.get_template('index.html')
        print(tmpl)
        return tmpl.render(salutation='Pittsburgh: ',
                           target='Temperature by Year',
                           title = "Weather",
                           plot = html,
                           sidebar = None,
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
