import requests
import re
from postgres.PostgreSQL import PostgreSQL
import json
import pandas as pd
import os
from collections import defaultdict
from collections import namedtuple
import numpy as np

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

# Special tuple type to store rows of our metadata
FetchableData = namedtuple("data", [
    'name', 'metadata_modified', 'file_format', 'url', 'file_id',
    'package_id', 'revision_id', 'resource_num'])


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
                name = result['name'],
                metadata_modified = result['metadata_modified'],
                file_format = resource['format'],
                url = resource['url'],
                file_id = resource['id'],
                package_id = resource['package_id'],
                revision_id = resource['revision_id'],
                resource_num = str(i),
                )
            output.append(row)
    return output


def needs_updating(flat_row):
    """Query the database to see if we should download new data"""

    # TODO: Hook up to database
    # Probably want to use id and and metadata_modified to check
    # database? Or maybe revision_id?
    return True


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


def fetch_file_by_url(url, basedir = "/tmp/pittsburgh", fname = None):
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


def insert_to_db(df, fname):
    dtypes  = [get_dtype(dtype) for dtype in df.dtypes]
    columns = [re.sub(' ', '_', col) for col in df.columns]
    filename, file_extension = os.path.splitext(fname)

    #Underscores only permissible non-alphanumeric
    filename = re.sub('[^a-zA-Z0-9]', '_', filename)

    df.fillna('NULL')
    with PostgreSQL(database = 'pittsburgh') as pg:
        pg.create_table(filename, cols = columns, types = dtypes)
        pg.add_rows(tuple(df.itertuples(index = False)))


def update_file_in_db(basedir, fname):
    """Use new file to update database"""

    #TODO actually update DB
    # Pandas is good about inferring types
    # and can interface directly with sqlalchemy
    # I was thinking we could use the name of the dataset as the table
    # name--Try to create the table based on inferred types. If it
    # already exists, add the new data to it.
    # Will need to figure out how to append vs update values
    
    print('update file in db')
    filename, file_extension = os.path.splitext(fname)

    if file_extension.lower() == '.csv':
        print('in if')
        df = pd.read_csv(os.path.join(basedir, fname))
        insert_to_db(df, fname)


def update_metadata_db(result):
    """Updates last fetched time in metadata DB"""

    # TODO Mark The file as newly update so we don't have to fetch it
    # again until it's update on the site
    pass
    

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

        if good_format and needs_updating(result):
            print('fetch')
            fetch_file_by_url(url, basedir = subdir, fname = fname)
            print('update')
            update_file_in_db(subdir, fname)
            # update_metadata_db(result)
        else:
            print("skipping {}".format(fname))



# def fetch_all_urls(urls, basedir = "/tmp/pittsburgh", fname = None):
#     for url in urls:
#         try:
#             fetch_file_by_url(url, basedir, fname)
#         except UnicodeEncodeError as e:
#             print(e)
#             print("Py2 problem, probably")

#         update_file_in_db(basedir, fname)
    
    
def main():
    metadata = fetch_metadata()
    parsed = extract_metadata(metadata)
    flat = flatten_extracted_data(parsed)
    try:
        os.mkdir(BASEDIR)
    except:
        pass
    fetch_files_by_type(flat, download_formats, basedir = BASEDIR)

# with PostgreSQL(database = 'pittsburgh') as psql:
#     types = TYPES
#     psql.create_table(table = 'weather', cols = cols, types = types)

    # for year in years:
    #     print(year)
    #     rows = parse_year_table(fetch_year_of_weather(year), year)
    #     with PostgreSQL(table = 'weather', database = 'pittsburgh') as psql:
    #         psql.add_rows(rows, types = types, cols = cols)


if __name__ == '__main__':
    main()


    # table = 'pittsburgh-police-arrest-data'
    # basedir = BASEDIR
    # f = pd.read_csv(os.path.join(basedir, fname))
    # dtypes = [DTYPE_CONVERSION[str(dtype)] for dtype in f.dtypes]
    # columns = f.columns
    # with PostgreSQL(database = 'pittsburgh') as pg:
    #     pg.create_table(table)
