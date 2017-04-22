from datetime import datetime, tzinfo, timedelta
import requests, json
import pandas as pd
from postgres.PostgreSQL import PostgreSQL

# Define class to offset UTC time
class Zone(tzinfo):
    def __init__(self,offset,isdst,name):
        self.offset = offset
        self.isdst = isdst
        self.name = name
    def utcoffset(self, dt):
        return timedelta(hours=self.offset) + self.dst(dt)
    def dst(self, dt):
            return timedelta(hours=1) if self.isdst else timedelta(0)
    def tzname(self,dt):
         return self.name
       
UTC = Zone(0,False,'UTC')
EST = Zone(-5,False,'EST')

# Convert Kelvin to Fahrenheit 
def convert_K_to_F(K):
    return round(1.8 * (K - 273) + 32,1)

# Convert UTC time to EST (or Pittsburgh) time
def convert_UTC_to_EST(my_date):
    my_date = datetime.strptime(my_date,'%Y-%m-%d %H:%M:%S')
    my_date = my_date.replace(tzinfo=UTC)
    my_date = my_date.astimezone(EST)
    return my_date

# Define column headers of basic weather forecast information
HEADERS =['Year', 'Month', 'Day', 'Hour', 'Temp', 'Events']

# Define types of each column
TYPES =['INT', 'INT', 'INT', 'INT', 'FLOAT', 'INT']

# Fetch the 5 day/3 hour weather forecasts from OpenWeatherMap
def fetch_weather_forecasts():
    # parameters for OpenWeatherMap API to get weather forecasts 
    cityID = "5206379" # Pittsburgh ID
    API_key = "a3e2ad7d3611478118790885a2e004ac"
    URL = " http://api.openweathermap.org/data/2.5/forecast?id=" + cityID + "&APPID=" + API_key
    # fetch the data
    response = requests.get(URL)
    data = json.loads(response.text)
#    data = testJSON
    wild_weather_codes = ['Thunderstorm','Rain','Snow','Extreme']
    forecasts = []
    for i in range(len(data['list'])):
        cl = data['list'][i]
        my_date = datetime.fromtimestamp(int(cl['dt']))
        my_date = convert_UTC_to_EST(str(my_date))
        # date1 = datetime.fromtimestamp(int(cl['dt'])).strftime('%Y-%m-%d %H:%M:%S')
        date_y = my_date.year
        date_m = my_date.month
        date_d = my_date.day
        date_h = my_date.hour
        temp = convert_K_to_F(cl['main']['temp'])
        weather = cl['weather'][0]['main']
        if weather in wild_weather_codes:
            events = 1
        else:
            events = 0
        forecast_i = [date_y, date_m, date_d, date_h, temp, events]
        forecasts.append(forecast_i)
    return forecasts

def main():
    with PostgreSQL(database = 'pittsburgh') as psql:
        cols = HEADERS
        types = TYPES
        psql.create_table(table = 'weather_forecasts', cols = cols, types = types)

    rows = fetch_weather_forecasts()
    with PostgreSQL(table = 'weather_forecasts', database = 'pittsburgh') as psql:
        psql.add_rows(rows, types = types, cols = cols)

    print('Success!')
    print('')
    print('Try running:')
    print('psql -U postgres pittsburgh')
    print('select * from weather_forecasts limit 5;')

if __name__ == '__main__':
    main()
