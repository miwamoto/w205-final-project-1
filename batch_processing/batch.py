from postgres.PostgreSQL import PostgreSQL
from sqlalchemy import create_engine
from collections import namedtuple
import datetime
import re

import pandas as pd
import numpy as np
engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5432/pittsburgh",
    isolation_level="READ UNCOMMITTED"
)
clean_engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5432/pittsburgh_clean",
    isolation_level="READ UNCOMMITTED"
)

MAX_AGE = 120
MIN_LAT = 40.3
MAX_LAT = 40.6
MIN_LONG = -80.1
MAX_LONG = -79.8

DTYPE_CONVERSION = {'int64': "INT", 'float64': "FLOAT", 'object': "VARCHAR", 'datetime64[ns]': 'VARCHAR'}

good_datasets = [    'weather_forecasts',
    'weather',
    'poverty',
    # 'allegheny_county_anxiety_medication_0',
    # 'allegheny_county_anxiety_medication_1',
    # 'allegheny_county_boundary_3',
    # 'allegheny_county_crash_data_15',
    # 'allegheny_county_depression_medication_0',
    # 'allegheny_county_depression_medication_1',
    # 'allegheny_county_diabetes_hospitalization_0',
    # 'allegheny_county_diabetes_hospitalization_1',
    # 'allegheny_county_fatal_accidental_overdoses_1',
    'allegheny_county_fatal_accidental_overdoses_2',
    # 'allegheny_county_hypertension_hospitalization_0',
    # 'allegheny_county_hypertension_hospitalization_1',
    # 'allegheny_county_jail_daily_census_0',
    # 'allegheny_county_land_cover_areas_3',
    # 'allegheny_county_municipal_boundaries_3',
    # 'allegheny_county_poor_housing_conditions_0',
    # 'allegheny_county_poor_housing_conditions_1',
    # 'allegheny_county_zip_code_boundaries_3',
    # 'autonomous_vehicle_survey_of_bicyclists_and_pedestrians_in_pitt',
    # 'diabetes_0',
    # 'diabetes_1',
    # 'hypertension_0',
    # 'hypertension_1',
    # 'metatable',
    'neighborhoods_with_snap_data_3d3a9_3',
    'non_traffic_citations_0',
    'pgh_snap_13',
    'pgh_snap_14',
    'pgh_snap_16',
    'pgh_snap_17',
    'pgh_snap_18',
    'pgh_snap_19',
    'pgh_snap_20',
    # 'pittsburgh_neighborhoods_64095_3',
    'pittsburgh_police_arrest_data_0',
    'pittsburgh_police_sectors_5f5e5_3',
    'pittsburgh_police_zones_4763f_3',
    'police_incident_blotter_30_day_0',
    'police_incident_blotter_archive_0',
    'police_incident_blotter_archive_2',
    # 'state_of_aging_in_allegheny_county_survey_2',
    # 'state_of_aging_in_allegheny_county_survey_3',


    'allegheny_county_anxiety_medication_0',
    'allegheny_county_anxiety_medication_1',
    'allegheny_county_boundary_3',
    'allegheny_county_crash_data_15',
    'allegheny_county_depression_medication_0',
    'allegheny_county_depression_medication_1',
    'allegheny_county_diabetes_hospitalization_0',
    'allegheny_county_diabetes_hospitalization_1',
    'allegheny_county_fatal_accidental_overdoses_1',
    'allegheny_county_fatal_accidental_overdoses_2',
    'allegheny_county_hypertension_hospitalization_0',
    'allegheny_county_hypertension_hospitalization_1',
    # 'allegheny_county_jail_daily_census_0',
    # 'allegheny_county_land_cover_areas_3',
    'allegheny_county_median_age_at_death_0',
    'allegheny_county_median_age_at_death_1',
    'allegheny_county_municipal_boundaries_3',
    'allegheny_county_obesity_rates_1',
    'allegheny_county_obesity_rates_2',
    'allegheny_county_poor_housing_conditions_0',
    'allegheny_county_poor_housing_conditions_1',
    'allegheny_county_primary_care_access_0',
    'allegheny_county_primary_care_access_1',
    'allegheny_county_smoking_rates_0',
    'allegheny_county_smoking_rates_1',
    'allegheny_county_zip_code_boundaries_3',
    # 'autonomous_vehicle_survey_of_bicyclists_and_pedestrians_in_pitt',
    'diabetes_0',
    'diabetes_1',
    # 'geocoders_0',
    'hyperlipidemia_0',
    'hyperlipidemia_1',
    'hypertension_0',
    'hypertension_1',
    # 'metatable',
    'neighborhoods_with_snap_data_3d3a9_3',
    'non_traffic_citations_0',
    'pgh_snap_13',
    'pgh_snap_14',
    'pgh_snap_16',
    'pgh_snap_17',
    'pgh_snap_18',
    'pgh_snap_19',
    'pgh_snap_20',
]

replacements = {
    'x': 'longitude',
    'long': 'longitude',
    'y': 'latitude',
    'lat': 'latitude',
    'pk': 'unique_id',
    'ccr': 'incident_number',
    'incidenttime': 'incident_time',
    'incidentlocation': 'incident_location',
    'incidentneighborhood': 'incident_neighborhood',
    'incidentzone': 'incident_zone',
    'incidenttract': 'incident_tract',
}

time_formats = {
    'police_incident_blotter_30_day_0': '%m/%d/%Y %H:%M',
    'police_incident_blotter_archive_0': '%m/%d/%Y %H:%M',
    'police_incident_blotter_archive_2': '%m/%d/%Y %H:%M',
    'allegheny_county_fatal_accidental_overdoses_2': '%H:%M %p',
    'non_traffic_citations_0': '%Y-%m-%dT%H:%M:%S',
    'pittsburgh_police_arrest_data_0': '%Y-%m-%dT%H:%M:%S',
    '': '%Y-%m-%dT%H:%M:%S',
    }
alt_time_formats = {
    'police_incident_blotter_30_day_0': '%Y-%m-%dT%H:%M:%S',
    'police_incident_blotter_archive_0': '%Y-%m-%dT%H:%M:%S',
}
date_formats = {
    'police_incident_blotter_30_day_0': '%m/%d/%Y %H:%M',
    'police_incident_blotter_archive_0': '%m/%d/%Y %H:%M',
    'police_incident_blotter_archive_2': '%m/%d/%Y %H:%M',
    'allegheny_county_fatal_accidental_overdoses_2': '%m/%d/%Y',
    }

def change_time(x):
    try:
        return datetime.datetime.strptime(x,time_formats[dset])
    except:
        try:
            return datetime.datetime.strptime(x,alt_time_formats[dset])
        except:
            try:
                return datetime.datetime.strptime(x,date_formats[dset])
            except:
                return np.nan
            
    


def infer_types(df, dset):
    for col in df.columns:
        try:
            df[col] = df[col].where(df[col] != '', np.nan)
        except:
            pass
        if col.endswith('time') or col.endswith('date'):
            col_prefix = re.sub('(date|time)', '', col)
            if 'time' in col:
                df[col] = df[col].apply(change_time)
                df[col_prefix + 'hour'] = df[col].apply(lambda x: x.hour)
                df[col_prefix + 'minute'] = df[col].apply(lambda x: x.minute)
            elif dset != 'weather':
                df[col] = df[col].apply(change_time)
            if col not in ('death_time',):
                df[col_prefix + 'month'] = df[col].apply(lambda x: x.month)
                df[col_prefix + 'day'] = df[col].apply(lambda x: x.day)
                df[col_prefix + 'year'] = df[col].apply(lambda x: x.year)
                df[col_prefix + 'day_of_year'] = df[col].apply(lambda x: x.dayofyear)
                df[col_prefix + 'week_of_year'] = df[col].apply(lambda x: x.weekofyear)
                df[col_prefix + 'weekday'] = df[col].apply(lambda x: x.weekday())
        try:
            df[col] = df[col].str.replace(',', '')
            df[col] = df[col].str.replace('$', '')
            df[col] = df[col].str.replace('%', '')
        except:
            pass
        try:
            df[col] = df[col].astype(np.float64)
        except:
            pass

        if col == 'age':
            df[col] = df[col].where(df[col] < MAX_AGE, np.nan)


def strip_chars(df):
    pass


def replace_columns(cols):
    cols = list(cols)
    for i, x in enumerate(cols):
        try:
            x = replacements[x]
        except:
            pass
        x = x.strip('_')
        x = re.sub('__*', '_', x)
        cols[i] = x if x != 'id' else '_id'
    return cols


def clean_coords(df, dset = ''):
    if 'latitude' in df.columns:
        df['latitude'] = df['latitude'].where(df['latitude'] != '', np.nan).astype(np.float64)
        df['latitude'] = df['latitude'].where(df['latitude'] > MIN_LAT, np.nan)
        df['latitude'] = df['latitude'].where(df['latitude'] < MAX_LAT, np.nan)
    if 'longitude' in df.columns:
        df['longitude'] = df['longitude'].where(df['longitude'] != '', np.nan).astype(np.float64)
        df['longitude'] = df['longitude'].where(df['longitude'] > MIN_LONG, np.nan)
        df['longitude'] = df['longitude'].where(df['longitude'] < MAX_LONG, np.nan)


def clean_df(df, dset):
    if dset == 'weather':
        df['year'] = df['year'].astype(np.int64)
        df['month'] = df['month'].astype(np.int64)
        df['day'] = df['day'].astype(np.int64)
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    
    if dset == 'police_incident_blotter_archive_2':
        df['ccr'] = df['ccr'].apply(lambda x: re.sub('[- ]', '', x))
    df.columns = replace_columns(df.columns)
    clean_coords(df, dset)
    infer_types(df, dset)
    return df
    

for i, dset in enumerate(good_datasets):
    print(i, dset)
    #
    with PostgreSQL(database = 'pittsburgh') as psql:
        try:
            psql.execute('drop table tmp')
        except:
            psql.conn.rollback()
        psql.execute('CREATE TABLE tmp as SELECT DISTINCT * from {}'.format(dset))
        
    df_buffer = pd.read_sql('tmp', engine, chunksize = 10000)
    for j, df in enumerate(df_buffer):
        if j > 0:
            print('{}0k records'.format(j))
        df = clean_df(df, dset)
        # if j == 0:
        #     types = [DTYPE_CONVERSION[str(x)] for x in df.dtypes]
        #     with PostgreSQL(database = 'pittsburgh_clean') as psql:
        #         psql.create_table(table = dset, types = types, cols = df.columns)
        
        
        df.to_sql(dset, clean_engine, if_exists = 'replace', index = False)

    if 'poverty' in good_datasets and 'non_traffic_citations_0' in good_datasets:
        with PostgreSQL(database = 'pittsburgh_clean') as psql:
            try:
                psql.execute('drop table citations_poverty')
            except:
                psql.conn.rollback()
            try:
                psql.execute('create table citations_poverty as \
                            select * from non_traffic_citations_0 c \
                            inner join poverty p \
                            using (neighborhood)')
            except:
                psql.conn.rollback()
