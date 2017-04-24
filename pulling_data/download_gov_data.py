
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
download_formats = ('CSV', 'ZIP')

whitelist = None

# Unnecessary and large
blacklist = {
    # Possibly useful, but MASSIVE
    #######################################
    'allegheny_county_jail_daily_census', #
    #######################################

    'allegheny_county_soil_type_areas',
    'pittsburgh_steps_map',
    'allegheny_county_cell_tower_points',
    'allegheny_county_restaurant_food_facility_inspection_violations',
    'allegheny_county_block_areas',
    'allegheny_county_dam_locations',
    'allegheny_county_supermarkets_convenience_stores',
    'pittsburgh_individual_historic_properties_6bd95',
    'port_authority_of_allegheny_county_park_and_rides',
    '311_data_in_development',
    'allegheny_county_park_rangers_outreach',
    'pittsburgh_bridges_18da5',
    'washington_county_crash_data',
    'allegheny_county_clean_indoor_air_act_exemptions',
    'allegheny_county_tobacco_vendors',
    'public_works_block_party_summary',
    'allegheny_county_address_points',
    'allegheny_county_plumbers',
    'allegheny_county_street_centerlines',
    'new_zoning_and_parcel_map_viewer',
    'pittsburgh_city_facilities_ba1f1',
    'signalized_intersections',
    'healthy_ride_trip_data',
    'allegheny_county_owned_roads_centerlines',
    'allegheny_county_municipal_building_energy_and_water_use',
    'allegheny_county_wic_vendor_locations',
    'allegheny_county_department_of_public_works_maintenance_district_boundaries',
    'allegheny_county_beltway_system_street_centerlines',
    'pghodsteps_b3a41',
    'allegheny_county_toxics_release_inventory',
    'allegheny_county_fire_districts',
    'allegheny_county_property_sale_transactions',
    'beaver_county_crash_data',
    'allegheny_county_property_viewer',
    'allegheny_county_building_footprint_locations_88dbb',
    'pittsburgh_traffic_signals_bbc6c',
    'allegheny_county_commercial_vehicle_inspections',
    'interactive_zoning_and_parcel_map',
    'water_features_3639f',
    'on_road_bicycle_pavement_markings',
    'westmoreland_county_crash_data',
    'allegheny_county_particulate_matter_2_5',
    'allegheny_county_traffic_counts',
    'allegheny_county_pennsylvania_senate_district_boundaries',
    'pghodfirezones',
    'pittsburgh_greenways_176b9',
    'permitted_shelters',
    'allegheny_county_watershed_boundaries',
    'pittsburgh_city_council_districts_5d3ed',
    'allegheny_county_voting_district_boundaries_fall_2016_present',
    'operations_green_spaces',
    'allegheny_county_polling_place_locations_november_2015',
    'allegheny_county_hydrology_areas',
    'snow_plow_activity_2015_2016',
    'capital_projects',
    'pittsburgh_ems_districts_58874',
    'allegheny_county_tif_boundaries_3b4b9',
    'allegheny_county_asbestos_permits',
    'pittsburgh_undermined_areas_191a3',
    'pittsburgh_fema_flood_zones_78814',
    'pittsburgh_fish_fry_locations',
    '2014_capital_project_budgets',
    'allegheny_county_election_results',
    'baldwin_borough_monthly_revenue_and_expenses',
    'allegheny_county_food_facilities',
    'allegheny_county_polling_place_locations_november_2014',
    'allegheny_county_dog_licenses',
    'pittsburgh_wards_map',
    'allegheny_county_council_districts',
    'street_sweeping_routes_5ac4c',
    'allegheny_county_parcel_boundaries',
    'allegheny_county_owned_bridges_points',
    'pittsburgh_city_boundary_cbe8f',
    'pittsburgh_2014_cdbg_block_groups_acc69',
    'lobbyist_activity',
    'field_listings',
    'allegheny_county_major_rivers',
    'pittsburgh_landslide_prone_3c79c',
    'pittsburgh_uptown_ipod_zoning_district_34233',
    'allegheny_county_wooded_area_boundaries',
    'port_authority_of_allegheny_county_transit_stops',
    'allegheny_county_tif_exclusions_9adaf',
    'make_my_trip_count_2015',
    'allegheny_county_cemetery_outlines',
    'allegheny_county_sheriff_sales',
    'port_authority_of_allegheny_county_transit_routes',
    'aced_federal_grant_contractor_tracking',
    'pittsburgh_index_map',
    'pittsburgh_ura_main_st_c626d',
    'play_area_listings',
    'whos_your_neighborhood_planner_map',
    'allegheny_county_voting_district_2016_web_map',
    'municipal_building_energy_usage',
    'allegheny_county_pennsylvania_u_s_legislative_congressional_district_boundaries',
    'allegheny_county_greenways',
    'allegheny_county_basin_outlines_map',
    'pittsburgh_historic_districts_5d240',
    'allegheny_county_map_index_grid',
    'allegheny_county_voting_district_2015_web_map',
    'pittsburgh_dpw_environmental_services_divisions_f4682',
    'pittsburgh_beautify_the_burgh_a4e12',
    '2010_census_blocks_with_geographic_codes_southwestern_pa',
    'allegheny_county_housing_and_community_environment_inspections',
    'healthy_ride_stations',
    'allegheny_county_land_use_areas',
    'allegheny_county_illegal_dump_sites',
    'allegheny_county_environmental_justice_areas',
    'allegheny_county_owned_bridges_centerlines',
    'butler_county_crash_data',
    'allegheny_county_crash_data',
    'allegheny_county_kane_regional_center_census',
    'allegheny_county_hydrology_lines',
    'summer_meal_sites',
    'allegheny_county_snow_route_centerlines_2016_2017',
    'regional_park_n_ride_facilities_web_inventory_click_download_to_view',
    'city_facilities_centroids',
    'pittsburgh_dpw_divisions_54e37',
    'allegheny_county_municipal_land_use_ordinances',
    'pittsburgh_2014_cdbg_census_tracts_09e99',
    'allegheny_county_polling_place_locations_november_2016',
    'pittsburgh_city_council_district_map',
    'allegheny_county_public_swimming_pool_hot_tub_and_spa_inspections',
    'green_infrastructure_siting_and_cost_effectiveness_analysis',
    'allegheny_county_school_district_boundaries',
    'aced_allegheny_home_improvement_loan_program_ahilp',
    'allegheny_county_air_quality',
    'allegheny_county_blazed_trails_locations',
}

maps = {
    'movepgh_project_map',
    'geocoders',
    'pittsburgh_map',
}

map_dsets = {
    'pittsburgh_Neighborhoods_map',
    'pittsburgh_neighborhoods_64095',
}

boundaries = {
    'allegheny_county_municipal_boundaries',
    'allegheny_county_zip_code_boundaries',
    'allegheny_county_boundary',
}

police_dsets = {
    'officer_training',
    'police_incident_blotter_archive',
    'pittsburgh_police_arrest_data',
    'police_incident_blotter_30_day',
    'pittsburgh_police_zones_4763f',
    'police_community_outreach',
    'police_civil_actions',
    'pittsburgh_police_sectors_5f5e5',
    'pittsburgh_police_zones_4763f',
    'non_traffic_citations',
    'pittsburgh_police_arrest_data_0',
    'pittsburgh_police_sectors_5f5e5_3',
    'pittsburgh_police_zones_4763f_3',
    'police_incident_blotter_30_day_0',
    'police_incident_blotter_archive_0',
    'police_incident_blotter_archive_2',
    'non_traffic_citations_0',
}

health_dsets = {
    'state_of_aging_in_allegheny_county_survey',
    'allegheny_county_primary_care_access',
    'allegheny_county_smoking_rates',
    'hyperlipidemia',
    'diabetes',
    'allegheny_county_obesity_rates',
    'hypertension',
    'allegheny_county_diabetes_hospitalization',
    'allegheny_county_median_age_at_death',
    'allegheny_county_anxiety_medication',
    'allegheny_county_depression_medication',
    'allegheny_county_fatal_accidental_overdoses',
    'allegheny_county_fatal_accidental_overdoses_2'
    'allegheny_county_hospitals',
    'allegheny_county_hypertension_hospitalization',
}

poverty_dsets = {
    'allegheny_county_poor_housing_conditions',
    'neighborhoods_with_snap_data_3d3a9',
    'pgh_snap',
    'pgh_snap_13',
    'pgh_snap_14',
    'pgh_snap_16',
    'pgh_snap_17',
    'pgh_snap_18',
    'pgh_snap_19',
    'pgh_snap_20',    
}

other_dsets = {
    'allegheny_county_fast_food_establishments',
}

cycling_dsets = {
    'current_bike_availability_by_station',
    'autonomous_vehicle_survey_of_bicyclists_and_pedestrians_in_pittsburgh_2017',
    'bike_rack_locations_downtown_pittsburgh',
    }

public_building_dsets = {
    'allegheny_county_magisterial_districts_outlines_2015',
    'pittsburgh_pli_violations_report',
    'allegheny_county_older_housing',
    'allegheny_county_public_building_locations',
    'allegheny_county_vacant_properties',
    'city_owned_publicly_available_properties',
    'library_locations',
    'allegheny_county_housing_tenure',
    'pittsburgh_zoning_districts_7200c',
    'envision_downtown_public_space_public_life_survey',
    }

green_dsets = {
    'allegheny_county_land_cover_areas',
    'allegheny_county_parks_data_web_map',
    'allegheny_county_parks_outlines_9416b',
    'comprehensive_parks_list',
    'pittsburgh_city_trees_84f6b',
}

political_dsets = {
    'allegheny_county_pa_house_of_representative_districts',
    'allegheny_county_pa_senate_districts',
    'allegheny_county_pennsylvania_house_of_representatives_district_boundaries',
    'allegheny_county_voting_district_boundaries_spring_2015_spring_2016',
    'campaign_finance_contributions',
    '2011_2015_city_of_pittsburgh_operating_budget',
    'uncertified_presidential_election_results_allegheny_county_general_election',
}

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
    fname = os.path.basename(fpath)
    fname, file_ext = os.path.splitext(fname)
    try:
        with open(fpath, 'w') as f:
            f.write(r.text)
            print('Fetched: {}'.format(fpath))
    except UnicodeEncodeError as e:
        with open(fpath, 'w') as f:
            try:
                f.write(r.text.encode('UTF-8'))
                print('Fetched: {}'.format(fname))
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


def insert_to_db(metadata, basedir, fname):
    reader = csv.reader(open(os.path.join(basedir,fname), 'r'))
    try:
        columns = reader.__next__()
    except:
        columns = reader.next()

    dtypes  = ['TEXT' for _ in columns]
    columns = [re.sub('[^a-zA-z0-9]', '_', col).lower() for col in columns]
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
        with PostgreSQL(database = 'pittsburgh') as pg:
            pg.create_table(table_name, cols = columns, types = dtypes)
            pg.add_rows(reader)
            md = list(metadata)
            md[-1] = table_name
            metadata = FetchableData(*md)
            update_metadata_db(metadata)
    except MemoryError as e:
        print(e)
        print('{} too big!!!!'.format(fname))


def clean_df(df):
    return df

# print('cleaning')
#     for i, x in enumerate(df.dtypes):
#         print(i)
#         if str(x) == 'object':
#             col = df.columns[i]
#             try:
#                 df[col + '_num'] = df[col].apply(lambda x: float(re.sub('[^0-9]', '', x)))
#             except Exception as e:
#                 pass
            
#     return df


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
        # try:
        #     # df = pd.read_csv(os.path.join(basedir, fname))
        # except:
        #     print("Couldn't parse csv: {}".format(fname))
        #     return
        # try:
        #     df = clean_df(df)
        # except:
        #     pass
        # insert_to_db(metadata, df, fname)
        insert_to_db(metadata, basedir, fname)
        

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
            pass
        # print("skipping {}".format(fname))


def main():
    metadata = fetch_metadata()
    parsed = extract_metadata(metadata)
    flat = flatten_extracted_data(parsed)
    try:
        os.mkdir(BASEDIR)
    except:
        pass

    whitelist = police_dsets | poverty_dsets | boundaries | map_dsets | health_dsets
    
    if whitelist is not None:
        flat = [d for d in flat if d.name in whitelist]
    elif blacklist is not None:
        flat = [d for d in flat if d.name not in blacklist]

    fetch_files_by_type(flat, download_formats, basedir = BASEDIR)


if __name__ == '__main__':
    main()

