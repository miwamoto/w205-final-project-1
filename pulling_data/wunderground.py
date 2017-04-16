from bs4 import BeautifulSoup as bs
import requests
import re
from postgres.PostgreSQL import PostgreSQL

MONTHS = {'Jan': 1,
          'Feb': 2,
          'Mar': 3,
          'Apr': 4,
          'May': 5,
          'Jun': 6,
          'Jul': 7,
          'Aug': 8,
          'Sep': 9,
          'Oct': 10,
          'Nov': 11,
          'Dec': 12}

# Header is not easily parsed from site
HEADER = ['Year', 'Month', 'Day',
  'Temp. (F) High', 'Temp. (F) Avg', 'Temp. (F) Low',
  'Dew Point (F) High', 'Dew Point (F) Avg', 'Dew Point (F) Low',
  'Humidity (%) High', 'Humidity (%) Avg', 'Humidity (%) Low',
  'Sea Level Press. (in) High', 'Sea Level Press. (in) Avg', 'Sea Level Press. (in) Low',
  'Visibility (mi) High', 'Visibility (mi) Avg', 'Visibility (mi) Low',
  'Wind (mph) High', 'Wind (mph) Avg', 'Wind (mph) Low',
  'Precip. (in)', 'Events']

# Define the types for each column
TYPES = [
    'INT', 'INT', 'INT', 
    'INT', 'INT', 'INT', 
    'INT', 'INT', 'INT', 
    'INT', 'INT', 'INT', 
    'FLOAT', 'FLOAT', 'FLOAT', 
    'INT', 'INT', 'INT', 
    'INT', 'INT', 'INT', 
    'FLOAT', 'TEXT']


def fetch_year_of_weather(year, daily = True):
    """Fetch daily or summary stats for a year in Pittsburgh"""

    url = 'https://www.wunderground.com/history/airport/KAGC/{}/1/1/CustomHistory.html?dayend=31&monthend=12&yearend={}&req_city=&req_state=&req_statename=&reqdb.zip=&reqdb.magic=&reqdb.wmo=&MR=1'.format(year, year)

    # Get html and get both table elements from page
    r = requests.get(url)
    soup = bs(r.text, 'html.parser')
    tables = soup.find_all('table')

    # Table 0 has summary statistics
    # Table 1 has daily statistics
    if daily:
        return tables[1]
    else:
        return tables[0]


def parse_year_table(table, year):
    cur_month = None
    trs = []

    for i, tr in enumerate(table.find_all('tr')):
        tds = []
        for j, td in enumerate(tr.find_all('td')):
            # Get text of td
            tdt = parse_td(td)

            # Pull out current month for later
            # and skip the rest of the line
            if tdt in MONTHS.keys():
                cur_month = MONTHS[tdt]
                break

            # Append month to beginning of each row
            if len(tds) == 0:
                tds.append(year)
                tds.append(cur_month)

            # Then add each other column
            tds.append(tdt)

        # Ignore blank rows
        if any(tds):
            trs.append(tds)

    return trs
    

def parse_td(td):
    """Parse td (or th) to get text"""

    # Strip out annoying characters
    tdt = re.sub('\n', '', td.text)
    tdt = re.sub('\t', '', tdt)
    tdt = tdt.strip()

    # Denotes missing value
    if tdt == '-' or tdt == '':
        return 'NULL'

    # Parse text out of the spans/etc
    if 'span' in tdt:
        return td.find('span').text
    elif 'href' in tdt:
        return td.find('a').text 
    else:
        return tdt


def main():
    cur_month = None
    years = range(1990, 2018)

    with PostgreSQL(database = 'pittsburgh') as psql:
        cols = [re.sub('[()\.%]','',col) for col in HEADER]
        cols = [re.sub(' ','_',col) for col in cols]
        types = TYPES
        psql.create_table(table = 'weather', cols = cols, types = types)

    for year in years:
        print(year)
        rows = parse_year_table(fetch_year_of_weather(year), year)
        with PostgreSQL(table = 'weather', database = 'pittsburgh') as psql:
            psql.add_rows(rows, types = types, cols = cols)

    print('Success!')
    print('')
    print('Try running:')
    print('psql -U postgres pittsburgh')
    print('select * from weather limit 5;')

if __name__ == '__main__':
    main()
