from postgres.PostgreSQL import PostgreSQL
import pandas as pd
import numpy as np

path = "/data/forecasts/"

def main():
    with PostgreSQL(database = 'pittsburgh') as psql:
        query = "SELECT incidentneighborhood, x, y FROM police_incident_blotter_archive_2"
        psql.execute(query)
        rows = psql.cur.fetchall()

    # place crime data into a DataFrame
    XY_df = pd.DataFrame(rows, columns=['neighborhood','x','y'])
    XY_df = XY_df[XY_df['x']!='NaN']
    XY_df = XY_df[XY_df['y']!='NaN']

    XY_df['X'] = pd.to_numeric(XY_df['x'])
    XY_df['Y'] = pd.to_numeric(XY_df['y'])

    # count incidents by date and neighborhood
    aveXY_df = XY_df.groupby(['neighborhood'], as_index=False)[['X','Y']].mean()
    aveXY_df = aveXY_df[aveXY_df['neighborhood']!='']
    aveXY_df

    try:
        os.mkdir(path)
    except:
        pass

    aveXY_df.to_csv(path + 'coordinates.csv')

if __name__ == '__main__':
    main()


