import requests
import json
from postgres.PostgreSQL import PostgreSQL

# Short-form column headers of demographic information
HEADERS =['Neighborhood', 'Sector', 'Population', 'Num_Poverty', 'Percent_Poverty']

# Define types of each column
TYPES =['VARCHAR', 'INT', 'INT', 'INT', 'INT', 'FLOAT']

def fetch_poverty_data():
    # Parameters for the Western Pennsylvania Regional Data Center API
    URL = 'https://data.wprdc.org/api/action/datastore_search?resource_id=86fab672-1c6b-48c3-a637-e5827c66ee5c'
    response = requests.get(URL)
    data = json.loads(response.text)
    records = data['result']['records']

    # Actual column names
    columns = ['Neighborhood','Sector #','Est. Pop. for which Poverty Calc. (2010)','Est. Pop. Under Poverty (2010)','Est. Percent Under Poverty (2010)']
    
    # Empty list to collect data
    rows = []

    # Collect the specified fields in 'columns'
    for record in records:
        my_fields = []
        for col in columns:
            if col == columns[-1]:
                my_fields.append(round(float(record[col]),4))
            else:
                my_fields.append(record[col])
        rows.append(my_fields)
    return rows

def main():
    # Create poverty table
    with PostgreSQL(database = 'pittsburgh') as psql:
        cols = HEADERS
        types = TYPES
        psql.create_table(table = 'poverty', cols = cols, types = types)

    # fetch data and populate the poverty table
    rows = fetch_poverty_data()
    with PostgreSQL(table = 'poverty', database = 'pittsburgh') as psql:
        psql.add_rows(rows, types = types, cols = cols)

    print('Success!')
    print('')
    print('Try running:')
    print('psql -U postgres pittsburgh')
    print('select * from poverty limit 5;')

if __name__ == '__main__':
    main()
