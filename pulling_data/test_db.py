from postgres.PostgreSQL import PostgreSQL
import pandas as pd
import csv

reader = csv.reader(open('pittsburgh_weather.csv', 'r'))
HEADER = reader.__NEXT__()

TYPES = [
    'INT', 'CHAR(3)', 'INT', 
    'INT', 'INT', 'INT', 
    'INT', 'INT', 'INT', 
    'INT', 'INT', 'INT', 
    'FLOAT', 'FLOAT', 'FLOAT', 
    'INT', 'INT', 'INT', 
    'INT', 'INT', 'INT', 
    'FLOAT', 'TEXT']
    

# with PostgreSQL('weather', database = 'pittsburgh') as psql:
#     cols = ('a', 'b', 'c')
#     types = ('INT', 'TEXT', 'CHAR(40)')
#     psql.create_table('weather', cols = cols, types = types)
#     print(psql.select())
#     psql.add_rows([[1, 'abc', 'def', ],[2, 'xxxabc', 'yyydef', ]], types = types, cols = cols)
#     print(psql.select())
