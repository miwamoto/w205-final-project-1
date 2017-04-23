from postgres.PostgreSQL import PostgreSQL
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression
import os, sys

path = "/data/forecasts/"

def create_forecasts(path, mymap):
    n = sorted(mymap.keys())
    num = len(n)
    results = np.random.rand(num,5)
    with open(path + 'random_forecasts.csv', 'w') as myfile:
        for i in range(num):
            my_str = n[i]
            for j in range(5):
                my_str += ',' + str(results[i][j])
            myfile.write(my_str + '\n')

def main():
    # call for crime incident data
    with PostgreSQL(database = 'pittsburgh') as psql:
        query = "SELECT incidenttime, incidentneighborhood FROM police_incident_blotter_archive_2"
        psql.execute(query)
        rows = psql.cur.fetchall()
    
    print('Retrieved crime data')

    # place crime data into a DataFrame
    crime_df = pd.DataFrame(rows, columns=['incidenttime', 'neighborhood'])

    # parse the string dates into datetime format and create various date-based features
    dateparse = lambda x: pd.datetime.strptime(x, '%m/%d/%Y %H:%M')
    crime_df['datetime'] = crime_df['incidenttime'].apply(dateparse)
    crime_df['date'] = crime_df['datetime'].map(lambda x: x.date())
    crime_df['year'] = crime_df['datetime'].map(lambda x: int(x.strftime('%Y')))
    crime_df['month'] = crime_df['datetime'].map(lambda x: int(x.strftime('%m')))
    crime_df['day'] = crime_df['datetime'].map(lambda x: int(x.strftime('%d')))
    crime_df['dayofweek'] = crime_df['datetime'].map(lambda x: x.dayofweek)
    crime_df['ymd'] = crime_df['datetime'].map(lambda x: int(10000*x.year + 100*x.month + x.day))
    crime_df['key'] = crime_df['ymd'].map(str) + crime_df['neighborhood']

    # create weekday dummies
    weekday_dummies = pd.get_dummies(crime_df['dayofweek'])
    weekday_dummies = weekday_dummies.rename(columns={0:'mon', 1:'tue', 2:'wed', 3:'thu', 4:'fri', 5:'sat', 6:'sun'})

    # create month dummies
    month_dummies = pd.get_dummies(crime_df['month'])
    month_dummies = month_dummies.rename(columns={1:'jan', 2:'feb', 3:'mar', 4:'apr', 5:'may', 6:'jun', 7:'jul', 8:'aug', 9:'sep', 10:'oct', 11:'nov', 12:'dec'})

    # join dummies to crime_df
    crime_df = pd.concat([crime_df,weekday_dummies,month_dummies], axis=1)

    # count incidents by date and neighborhood
    agg_df = crime_df.groupby(['ymd','neighborhood'], as_index=False)['incidenttime'].count()
    agg_df = agg_df.rename(columns={'incidenttime':'num_incidents'})
    agg_df['key'] = agg_df['ymd'].map(str) + agg_df['neighborhood']

    # feature creation Jan 1 dummy
    agg_df['jan1'] = agg_df['ymd'].map(lambda x: np.where(str(x)[-4:]=='0101',1,0))

    # feature creation 1st of month dummy
    agg_df['1stm'] = agg_df['ymd'].map(lambda x: np.where(str(x)[-2:]=='01',1,0))

    # feature creation 15th of month dummy
    agg_df['15thm'] = agg_df['ymd'].map(lambda x: np.where(str(x)[-2:]=='15',1,0))

    # feature creation Dec 25 dummy
    agg_df['dec25'] = agg_df['ymd'].map(lambda x: np.where(str(x)[-4:]=='1225',1,0))

    # add dummies to aggregates
    dummies = ['key','date','mon','tue','wed','thu','fri','sat','sun','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    agg_df = pd.merge(left=agg_df, right=crime_df[dummies], on=['key'], how='left')

    print('Aggregated crime data and created features')

    # query for poverty data
    with PostgreSQL(database = 'pittsburgh') as psql:
        query = "SELECT neighborhood, percent_poverty FROM poverty"
        psql.execute(query)
        rows = psql.cur.fetchall()

    # place poverty data into a DataFrame
    poverty_df = pd.DataFrame(rows, columns=['neighborhood', 'percent_poverty'])

    # join poverty to aggregates
    agg_df = pd.merge(left=agg_df, right=poverty_df, on=['neighborhood'], how='left')

    print('Retrieved and joined demographic data')

    # query for weather data
    with PostgreSQL(database = 'pittsburgh') as psql:
        query = "SELECT year, month, day, temp_f_high, events FROM weather WHERE year > 2005"
        psql.execute(query)
        rows = psql.cur.fetchall()

    # place weather data into a DataFrame
    weather_df = pd.DataFrame(rows, columns=['year', 'month', 'day', 'temp_high', 'events'])
    weather_df['ymd'] = 10000 * weather_df['year'] + 100 * weather_df['month'] + weather_df['day']
    weather_df['weather'] = weather_df['events'].map(lambda x: np.where(x=='NULL',1,0))

    # join weather to aggregates
    columns = ['ymd','temp_high','weather']
    agg_df = pd.merge(left=agg_df, right=weather_df[columns], on=['ymd'], how='left')

    print('Retrieved, transformed and joined weather data')

    # add neighborhood numbers
    mymap = {}
    nbh = sorted(list(set(list(agg_df['neighborhood']))))
    for i in range(len(nbh)):
        mymap[nbh[i]] = i+1
    agg_df['nbh'] = agg_df['neighborhood'].map(lambda x: mymap[x])

    # create final table of feature by removing duplicate rows
    agg_df = agg_df.drop_duplicates()
    agg_df = agg_df.reset_index(drop=True)
    
    df = agg_df[np.isfinite(agg_df['percent_poverty'])]
    df = df[np.isfinite(df['temp_high'])]

    print('Clean data for null or infinity values')

    y = df['num_incidents']
    features = ['nbh', 'jan1', '1stm', '15thm', 'dec25', 'mon', 'tue', 'wed', 'thu', 'fri',
           'sat', 'sun', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul',
           'aug', 'sep', 'oct', 'nov', 'dec', 'percent_poverty', 'temp_high', 'weather']
    X = df[features]

    lr = LinearRegression()
    model = lr.fit(X,y)

    print('Fitted forecast model')

    try:
        os.mkdir(path)
    except:
        pass

    with open(path + 'forecasts.csv', 'w') as myfile:
        for i in list(model.coef_):
            myfile.write(str(i) + '\n')                                          
                                             
    create_forecasts(path, mymap)

    print('Success!')
    print('Written forecasts to files in /data/forecasts')
                                             
if __name__ == '__main__':
    main()
