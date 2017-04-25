from postgres.PostgreSQL import PostgreSQL
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.naive_bayes import MultinomialNB
import os, sys
from sqlalchemy import create_engine

clean_engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5432/pittsburgh_clean",
    isolation_level="READ UNCOMMITTED"
)


path = "/data/forecasts/"

def get_value_from_wf_df(df,ymd,col):
    return list(df[df['ymd']==ymd][col])[0]

def get_neighborhood_poverty(df, nbh):
    pov = list(df[df['neighborhood']==nbh]['percent_poverty'])
    if len(pov) == 1:
        return pov[0]
    else:
        return 'NA'

def get_X_Y(df, nbh):
    X = list(df[df['neighborhood']==nbh]['X'])
    Y = list(df[df['neighborhood']==nbh]['Y'])
    return X, Y
    
def create_dummies(vect,v,n):
    for i in range(n):
        if i == v:
            vect.append(1)
        else:
            vect.append(0)
    return vect

def ymd_check(vect,ymd,v):
    if ymd==v:
        vect.append(1)
    else:
        vect.append(0)
    return vect

def main():
    crime_df = pd.read_sql('police_incident_blotter_archive_2', clean_engine)

    print('Retrieved crime data')

    crime_df.columns = ['unique_id', 'incident_number', 'hierarchy',
    'datetime', 'incident_location', 'clearedflag',
    'neighborhood', 'zone', 'hierarchydesc', 'offenses', 'tract',
    'longitude', 'latitude', 'hour', 'minute', 'month', 'day', 'year',
    'day_of_year', 'week_of_year', 'dayofweek']


    # parse the string dates into datetime format and create various date-based features
    crime_df['datetime'] = pd.to_datetime(crime_df['datetime'])
    crime_df['date'] = crime_df['datetime'].map(lambda x: x.date())
    crime_df['ymd'] = crime_df['year'] * 10000 + crime_df['month'] * 100 + crime_df['day']
    crime_df['key'] = crime_df['ymd'].map(str) + crime_df['neighborhood']

    # create weekday dummies
    weekday_dummies = pd.get_dummies(crime_df['dayofweek'])
    weekday_dummies = weekday_dummies.rename(columns={0:'mon', 1:'tue', 2:'wed',
                                                      3:'thu', 4:'fri', 5:'sat', 6:'sun'})

    # create month dummies
    month_dummies = pd.get_dummies(crime_df['month'])
    month_dummies = month_dummies.rename(columns={1:'jan', 2:'feb', 3:'mar', 4:'apr',
                    5:'may', 6:'jun', 7:'jul', 8:'aug', 9:'sep', 10:'oct', 11:'nov', 12:'dec'})

    # join dummies to crime_df
    crime_df = pd.concat([crime_df,weekday_dummies,month_dummies], axis=1)

    # count incidents by date and neighborhood
    agg_df = crime_df.groupby(['ymd','neighborhood'], as_index=False)['datetime'].count()
    agg_df = agg_df.rename(columns={'datetime':'num_incidents'})
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
    dummies = ['key','date','mon','tue','wed','thu','fri','sat','sun',
               'jan','feb','mar','apr','may', 'jun','jul','aug','sep','oct','nov','dec']
    agg_df = pd.merge(left=agg_df, right=crime_df[dummies], on=['key'], how='left')

    print('Aggregated crime data and created features')

    # query for poverty data
    poverty_df = pd.read_sql('poverty', clean_engine, columns = ['neighborhood', 'percent_poverty'])

    # join poverty to aggregates
    agg_df = pd.merge(left=agg_df, right=poverty_df, on=['neighborhood'], how='left')

    print('Retrieved and joined demographic data')

    # query for weather data
    weather_df = pd.read_sql('weather', clean_engine, columns = ['year', 'month',
                                                                 'day', 'temp_f_high', 'events'])
    weather_df = weather_df[weather_df['year'] > 2004]
    
    weather_df['ymd'] = 10000 * weather_df['year'] + 100 * weather_df['month'] + weather_df['day']
    weather_df['weather'] = weather_df['events'].map(lambda x: np.where(x=='NULL',0,1))

    # join weather to aggregates
    columns = ['ymd','temp_f_high','weather']
    agg_df = pd.merge(left=agg_df, right=weather_df[columns], on=['ymd'], how='left')
    agg_df = agg_df[agg_df['neighborhood']!='']

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

    # create neighborhood dummies
    agg_df = pd.concat([agg_df, pd.get_dummies(agg_df['nbh'])], axis=1); agg_df

    df = agg_df[np.isfinite(agg_df['percent_poverty'])]
    df = df[np.isfinite(df['temp_f_high'])]

    print('Clean data for null or infinity values')

    y = df['num_incidents']
    # regression features (exclude 'sun' and 'dec' to avoid multicolinearity)
    features = ['percent_poverty', 'temp_f_high', 'weather', 'jan1', '1stm', '15thm', 'dec25',
                'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'jan',
           'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep',
           'oct', 'nov', 'dec' ]
    for key in mymap.keys():
        features.append(mymap[key])

    # exclude last neighborhood to avoid multicolinearity
    # features = features[:-1] 
    X = df[features]

    clf = MultinomialNB()
    clf.fit(X,y)

    print('Fitted forecast model')

    # Call for weather forecast data to create prediction vectors
    
    weather_forecasts_df = pd.read_sql('weather_forecasts', clean_engine, columns = ['year', 'month',
                                                'day', 'temp', 'events'])


    weather_forecasts_df['ymd'] = 10000 * weather_forecasts_df['year'] + 100 * weather_forecasts_df['month'] + weather_forecasts_df['day']

    wf_df = weather_forecasts_df.groupby(['ymd'], as_index=False)['temp'].max()
    events = weather_forecasts_df.groupby(['ymd'], as_index=False)['events'].count()
    events['weather'] = events['events'].map(lambda x: np.where(x>0,1,0))
    wf_df = pd.concat([wf_df,events['weather']], axis=1)

    # feature creation month
    wf_df['month'] = wf_df['ymd'].map(lambda x: int(str(x)[-4:-2]))

    # feature creation day of week
    dateparse = lambda x: pd.to_datetime(str(x), format='%Y%m%d')
    wf_df['datetime'] = wf_df['ymd'].apply(dateparse)
    wf_df['dayofweek'] = wf_df['datetime'].map(lambda x: x.dayofweek)


    XY_df = pd.read_sql('police_incident_blotter_archive_2', clean_engine,
                        columns = ['latitude', 'longitude', 'incident_neighborhood'])
    XY_df.columns = ['X', 'Y', 'neighborhood']
    XY_df = XY_df[XY_df['X']!=np.nan]
    XY_df = XY_df[XY_df['Y']!=np.nan]

    # count incidents by date and neighborhood
    aveXY_df = XY_df.groupby(['neighborhood'], as_index=False)[['X','Y']].mean()
    aveXY_df = aveXY_df[aveXY_df['neighborhood']!='']
    aveXY_df
    
    # get list of forecast dates
    ymd_list = list(wf_df['ymd'])

    if len(ymd_list) == 6:
        ymd_list = ymd_list[1:]

    # populate first row of results
    results = []
    result = ['Neighborhood','Y','X','P1','P2','P3','P4','P5']
                                                                                     
    results.append(result)
    for neighborhood in mymap.keys():

        # feature inputs
        nbh = mymap[neighborhood]
        max_nbh = np.max(list(mymap.values()))
        X, Y = get_X_Y(aveXY_df, neighborhood)
        poverty = get_neighborhood_poverty(poverty_df,neighborhood)
        
        # prediction result vector for neighborhood
        result = [neighborhood]
        result.append(X[0])
        result.append(Y[0])

        if poverty == 'NA': # can't make a forecast
            result.append('NA')
            result.append('NA')
            result.append('NA')
            result.append('NA')
            result.append('NA')
        else:               
            for ymd in ymd_list:
                # feature inputs
                temp = get_value_from_wf_df(wf_df,ymd,'temp')
                weather = get_value_from_wf_df(wf_df,ymd,'weather')
                month = get_value_from_wf_df(wf_df,ymd,'month')
                dayofweek = get_value_from_wf_df(wf_df,ymd,'dayofweek')

                # create feature vector
                curr_vect = []
                # add poverty
                curr_vect.append(poverty)
                # add temp
                curr_vect.append(temp)
                # add weather
                curr_vect.append(weather)
                # Jan 1
                curr_vect = ymd_check(curr_vect, str(ymd)[-4:], '0101')
                # 1st of month
                curr_vect = ymd_check(curr_vect, str(ymd)[-2:], '01')
                # 15th of month
                curr_vect = ymd_check(curr_vect, str(ymd)[-2:], '15')
                # Dec 25
                curr_vect = ymd_check(curr_vect, str(ymd)[-2:], '1225')
                # add weekday dummies
                curr_vect = create_dummies(curr_vect,dayofweek,7)
                # add month dummies
                curr_vect = create_dummies(curr_vect,month,12)
                # add weekday dummies
                curr_vect = create_dummies(curr_vect,nbh,max_nbh)
                # print(curr_vect)
                pred = 0
                try:
                    pred = round(1-clf.predict_proba(np.array(curr_vect).reshape(1, -1)).item(0),4)
                except:
                    pass
                result.append(pred)
        results.append(result)

    try:
        os.mkdir(path)
    except:
        pass

    print('Success!')

    result_df = pd.DataFrame(results[1:], columns= results[0], dtype = np.float64)


    for c in range(1,6):
        c = str(c)
        result_df['P' + c] = result_df['P' + c].where(
            result_df['P' + c] != 'NA', np.nan)
        result_df['P' + c] = result_df['P' + c].astype(np.float64)

    result_df.to_sql('crime_forecasts', clean_engine, if_exists = 'replace', index = None)
    print('Wrote forecasts to pittsburgh_clean db in table: crime_forecasts')

    with open(path + 'temp.csv', 'w') as myfile:
        for result in results:
            my_str = ''
            for i in result:
                my_str += str(i) + ','
            my_str = my_str[:-1]
            myfile.write(my_str + '\n')     
            
    df = pd.read_csv(path + 'temp.csv')
    df = df.dropna(how='any')
    df.to_csv(path + 'forecasts.csv')
                                             
    print('Wrote forecasts to files in /data/forecasts')
                                             
if __name__ == '__main__':
    main()
