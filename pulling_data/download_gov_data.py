import requests
import re
from postgres.PostgreSQL import PostgreSQL
import json
import pandas as pd
import os
from collections import defaultdict
from collections import namedtuple
import numpy as np
import csv

######################################################
# Parameters to retrieve and save data from data.gov #
######################################################

# URL to pull all Pittsburgh datasets
api_url = 'https://catalog.data.gov/api/3/action/package_search'
query_url = '?q=Allegheny%20County%20/%20City%20of%20Pittsburgh%20/%20Western%20PA%20Regional%20Data%20Center'
search_url = api_url + query_url
# arrest_url = 'https://data.wprdc.org/datastore/dump/e03a89dd-134a-4ee8-a2bd-62c40aeebc6f'

# In case we need to rename extensions
EXTENSIONS = {'CSV': 'csv', 'HTML': 'html', 'ZIP': 'zip'}

DTYPE_CONVERSION = {'int64': "INT", 'float64': "FLOAT", 'object': "VARCHAR"}

BINARY_FORMATS = ('ZIP', 'GIF', 'JPG')

# Where the data will be saved
BASEDIR = '/data/pb_files'

# Formats to download
download_formats = ('CSV',)

# Unnecessary and large
blacklist = None
whitelist = None

police_files = (
'police_incident_blotter_archive',
'pittsburgh_police_arrest_data',
'police_incident_blotter_30_day',
'pittsburgh_police_zones_4763f',
'police_community_outreach',
'police_civil_actions',
'pittsburgh_police_sectors_5f5e5',
)

# Special tuple type to store rows of our metadata
FetchableData = namedtuple("data", [
    'name', 'metadata_modified', 'file_format', 'url', 'file_id',
    'package_id', 'revision_id', 'resource_num', 'table'])


##############################
# Functions to retrieve data #
##############################

def fetch_metadata(url = search_url, rows = 1000):
    """Fetch daily or summary stats for a year in Pittsburgh"""

    url += '&rows={}'.format(rows)
    r = requests.post(url)
    my_json = json.loads(r.text)
    return my_json


def extract_metadata(md):
    """Pull useful things from each search result"""

    assert md['success'] is True
    results = md['result']['results']
    output = []
    for result in results:
        resources = result['resources']
        parsed = {}
        parsed['name'] = result['name']
        parsed['metadata_modified'] = result['metadata_modified']
        parsed['resources'] = []
        for res in resources:
            parsed['resources'].append({
                'format': res['format'],
                'url': res['url'],
                'id': res['id'],
                'package_id': res['package_id'],
                'revision_id': res['revision_id'],
                })
        output.append(parsed)
    return output

def flatten_extracted_data(extracted):
    output = []
    for result in extracted:
        for i, resource in enumerate(result['resources']):
            row = FetchableData(
                name = re.sub('[^a-zA-Z0-9]', '_', result['name']),
                metadata_modified = result['metadata_modified'],
                file_format = resource['format'],
                url = resource['url'],
                file_id = resource['id'],
                package_id = resource['package_id'],
                revision_id = resource['revision_id'],
                resource_num = str(i),
                table = None,
                )
            output.append(row)
    return output


def metacompare(metadata):
    #Query the database to see if we should download new data
    with PostgreSQL(database = 'pittsburgh') as psql:
        # Find the file id, retrieve its revision id
        query = "select revision_id, metadata_modified from metatable where file_id = '{}' limit 1".format(metadata.file_id)
        psql.execute(query)
        try:
        # Only one result returned, so take the 0th item
            result = psql.cur.fetchall()
        except psql.conn.ProgrammingError as e:
            return True

        try:
            revision_id = result[0][0]
            metadata_modified = result[0][1]
        except IndexError as e:
            return True

    # If current revision_id or metadata_modified doesn't match the
    # one in table mark file for download
    diff_modified = metadata.metadata_modified != metadata_modified
    diff_revision = metadata.revision_id != revision_id

    if diff_modified or diff_revision:
        return True
    else:
        return False

def update_metadata_db(metadata):
    """Updates last fetched time in metadata DB"""
    query1 = "INSERT INTO metatable \
            (file_id          , \
            revision_id       , \
            name              , \
            metadata_modified , \
            file_format       , \
            url               , \
            package_id        , \
            resource_num      , \
            table_name          \
            ) VALUES (  \
            '{}', '{}', '{}', '{}', '{}',\
            '{}', '{}', '{}', '{}')".format(
                metadata.file_id, 
                metadata.revision_id, 
                metadata.name, 
                metadata.metadata_modified, 
                metadata.file_format, 
                metadata.url, 
                metadata.package_id,
                metadata.resource_num,
                metadata.table,
            )
    

    query2 = "UPDATE metatable where file_id = '{}' set \
            revision_id = '{}', \
            metadata_modified = '{}', \
            url '{}'".format(
                metadata.file_id,
                metadata.revision_id,
                metadata.metadata_modified,
                metadata.url
            )

    with PostgreSQL(database = 'pittsburgh') as psql:
        psql.execute(query1, query2)


def write_binary(url, fpath):
    response = requests.get(url, stream=True)
    with open(fpath, 'wb') as f:
        if not response.ok:
            pass
    # Something went wrong
        else:
            for block in response.iter_content(1024):
                f.write(block)


def write_text(url, fpath):
    r = requests.get(url)
    try:
        with open(fpath, 'w') as f:
            f.write(r.text)
            print('Fetched: {}'.format(fpath))
    except UnicodeEncodeError as e:
        with open(fpath, 'w') as f:
            try:
                f.write(r.text.encode('UTF-8'))
                print('Fetched: {}'.format(fpath))
            except:
                print('failed. moving on')
    except MemoryError as e:
        try:
            write_binary(url, fpath)
        except:
            print('failed. moving on')


def fetch_file_by_url(metadata, url, basedir = "/tmp/pittsburgh", fname = None):
    """Save file from url in designated location"""
    if fname is None:
        fname = os.path.basename(url)
    fpath = os.path.join(basedir, fname)

    _, file_ext = os.path.splitext(fname)

    if file_ext[1:].upper() in BINARY_FORMATS:
        write_binary(url, fpath)
    else:
        write_text(url, fpath)


def get_dtype(dtype):
    try:
        dtype = DTYPE_CONVERSION[str(dtype)]
    except KeyError:
        dtype = 'STRING'
    return dtype


def insert_to_db(metadata, df, fname):
    dtypes  = [get_dtype(dtype) for dtype in df.dtypes]
    columns = [re.sub('[^a-zA-z0-9]', '_', col).lower() for col in df.columns]
    nums = [str(n) for n in range(10)]
    columns = ["_" + col if col[0] in nums else col for col in columns]
    filename, file_extension = os.path.splitext(fname)

    with PostgreSQL(database = 'pittsburgh') as psql:
        query = "select DISTINCT table_name from information_schema.columns where table_name like '{}%'".format(metadata.name)
        psql.execute(query)
        tables = [table[0] for table in psql.cur.fetchall()]

    table_name = filename
    for table in tables:
        with PostgreSQL(table = table, database = 'pittsburgh') as psql:
            query = "select column_name from information_schema.columns where table_name = '{}'".format(table)
            psql.execute(query)
            columnsOld = [col[0] for col in psql.cur.fetchall()]

        if columnsOld == columns:
            table_name = table
            break

    #make new table and add rows
    try:
        df = df.fillna('NULL')
        with PostgreSQL(database = 'pittsburgh') as pg:
            pg.create_table(table_name, cols = columns, types = dtypes)
            pg.add_rows(tuple(df.itertuples(index = False)))
        md = list(metadata)
        md[-1] = table_name
        metadata = FetchableData(*md)
        update_metadata_db(metadata)
    except MemoryError as e:
        print(e)
        print('{} too big!!!!'.format(fname))


def clean_df(df):
    return df
    print('cleaning')
    for i, x in enumerate(df.dtypes):
        print(i)
        if str(x) == 'object':
            col = df.columns[i]
            try:
                df[col + '_num'] = df[col].apply(lambda x: float(re.sub('[^0-9]', '', x)))
            except Exception as e:
                pass
                
    return df


def update_file_in_db(metadata, basedir, fname):
    """Use new file to update database"""

    #TODO actually update DB
    # Pandas is good about inferring types
    # and can interface directly with sqlalchemy
    # I was thinking we could use the name of the dataset as the table
    # name--Try to create the table based on inferred types. If it
    # already exists, add the new data to it.
    # Will need to figure out how to append vs update values
    
    filename, file_extension = os.path.splitext(fname)

    if file_extension.lower() == '.csv':
        try:
            df = pd.read_csv(os.path.join(basedir, fname))
        except:
            print("Couldn't parse csv: {}".format(fname))
            return
        try:
            df = clean_df(df)
        except:
            pass
        insert_to_db(metadata, df, fname)
    

def fetch_files_by_type(flat_results, data_formats = ("CSV", "KML"), basedir = '/data/pb_files'):
    """Fetch files of specified type"""

    urls = defaultdict(list)
    try:
        os.mkdir(basedir)
    except OSError as e:
        pass

    for result in flat_results:
        f_format  = result.file_format
        num       = result.resource_num
        url       = result.url
        name      = result.name

        try:
            ext = EXTENSIONS[f_format]
        except KeyError as e:
            ext = f_format

        subdir = os.path.join(basedir, f_format)

        try:
            os.mkdir(subdir)
        except OSError as e:
            pass

        # Only download if format is of specified type
        # or if no types specified
        good_format = data_formats is None or f_format in data_formats
        fname = '{}_{}.{}'.format(name, num, ext)
        fname, fext = os.path.splitext(fname)

        #Underscores only permissible non-alphanumeric
        fname = re.sub('[^a-zA-Z0-9]', '_', fname)
        fname = fname + fext


        if good_format and metacompare(result):
            fetch_file_by_url(result, url, basedir = subdir, fname = fname)
            if result.file_format.lower() in ('csv', '.csv'):
                update_file_in_db(result, subdir, fname)
        else:
            print("skipping {}".format(fname))


def main():
    metadata = fetch_metadata()
    parsed = extract_metadata(metadata)
    flat = flatten_extracted_data(parsed)
    try:
        os.mkdir(BASEDIR)
    except:
        pass

    whitelist = police_files
    
    if whitelist is not None:
        flat = [d for d in flat if d.name in whitelist]
    elif blacklist is not None:
        flat = [d for d in flat if d.name not in blacklist]

    fetch_files_by_type(flat, download_formats, basedir = BASEDIR)


if __name__ == '__main__':
    main()
