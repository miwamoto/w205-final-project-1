import os
import getpass
import json
from IPython.display import display
import arcgis
from arcgis.gis import GIS

### This ingests the Pittsburgh data, creates map layers and publishes to ArcGIS online
### These layers are useful for further geospatial analysis.


ACCOUNT = 'miwamotoDEV'
FORECASTFILE = 'forecasts.csv'
FORECASTPATH = ''
BASEDIR = '/data/pb_files'

def logon_to_arcgis():
    
    password = getpass.getpass('Enter password: ')
    try:
        global gis
        gis = GIS('https://www.arcgis.com','miwamotoDEV', password)
        print('Successfully logged in as: ' + gis.properties.user.username)
        return (True)
    except:
        print('Invalid password')
    return(False)


def save_layer(properties, filepath, thumbnail):
    try:
        content = gis.content.add(properties, data = filepath, thumbnail = thumbnail)
        print('Success ' + properties['title'] + ' imported into GIS')
        return(content)
    except:
        print('file may already exist')
        search_str = 'title: ' + properties['title']
        search_results = gis.content.search(search_str, 'Feature Layer')
        try:
            return(search_results[0])
            print('Success - ' + search_str + ' found')
        except:
            print(search_str + ' layer not found in GIS')


def testrun():

    csv_path = r'/data/pb_files/CSV/pittsburgh-police-arrest-data_0.csv'
    csv_properties = {'title': 'Pittsburgh Arrest Data',
                 'description':'Pittsburgh Arrest Data downloaded from data.gov',
                 'tags':'arcgis, python, Pittsburgh, arrests, crime'}
    thumbnail_path = r'handcuffs.png'
    
    crime_csv_item = save_layer(csv_properties, csv_path, thumbnail_path)
    
    ### Check layers
    crime_csv_item.layers
    crime_csv_item.tables
    crime_feature_layer = crime_csv_item.layers[0]

    ### Check layer properties
    crime_feature_layer.properties.extent

    ### Check layer capabilities
    crime_feature_layer.properties.capabilities

    ### List feature fields
    for f in crime_feature_layer.properties.fields:
        print(f['name'])
 

def main():
    
    test = False

    ### Log on to ArcGIS account  
    success = False
    attempt = 0 
    while not success and attempt <3:
        success = logon_to_arcgis()
        attempt += 1

    ### testing getting a layer
    if (test): testrun()
        
    
    ### Import Neighborhood GeoSpacial Info
    csv_path = BASEDIR + '/ZIP/pittsburgh_neighborhoods_64095_5.zip'
    csv_properties = {'title': 'Pittsburgh Neighborhoods',
                 'description':'Pittsburgh Neighborhood Shapefile downloaded from data.gov',
                 'tags':'arcgis, python, Pittsburgh, neighborhood'}
    thumbnail_path = r'neighborhood-icon.png'

    neighborhood_item = save_layer(csv_properties, csv_path, thumbnail_path)
    try:
        neighborhood_layer = neighborhood_item.publish()
        print('Success - published')
    except:
        print('Shapefile already published')
 

    ### Import Neighborhood GeoSpacial SNAP Data     
    csv_path = BASEDIR + '/ZIP/neighborhoods_with_snap_data_3d3a9_5.zip' 
    csv_properties = {'title': 'Pittsburgh Neighborhoods with SNAP Data',
                     'description':'Pittsburgh Neighborhood SNAP data downloaded from data.gov',
                     'tags':'arcgis, python, Pittsburgh, neighborhood'}
    thumbnail_path = 'neighborhood-icon.png'

    neighborhood_item = save_layer(csv_properties, csv_path, thumbnail_path)
    try:
        neighborhood_layer = neighborhood_item.publish()
        print('Success - published')
    except:
        print('CSV already published')
   

    ### Import latest Incident Probability Data
    csv_path = FORECASTPATH+ FORECASTFILE
    csv_properties = {'title': 'Pittsburgh Incident Forecast',
                 'description':'Pittsburgh Incident Forecast',
                 'tags':'arcgis, python, Pittsburgh, arrests, crime'}
    thumbnail_path = r'handcuffs.png'

    crime_csv_item = save_layer(csv_properties, csv_path, thumbnail_path)
    try:
        forecast_layer = crime_csv_item.publish()
        print('Success - published')
    except:
        print('Layer already published')
        
    ### layer manipulation
    #map1 = gis.map('Pittsburgh, PA', zoomlevel = 13)
    #search_results = gis.content.search('title: Pittsburgh Incident Forecast',
    #                                'Feature Layer')
    #search_results
    #Incident_item = search_results[0]
    #map1.add_layer(Incident_item)
    
        


if __name__ == '__main__':
    main()
