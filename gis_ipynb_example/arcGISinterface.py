import os
import getpass
import json
from IPython.display import display
import arcgis
from arcgis.gis import GIS

ACCOUNT = 'miwamotoDEV'


def logon_to_arcgis():
    
    password = getpass.getpass("Enter password: ")
    try:
        global gis
        gis = GIS("https://www.arcgis.com","miwamotoDEV", password)
        print("Successfully logged in as: " + gis.properties.user.username)
        return (True)
    except:
        print('Invalid password')
    return(False)


def save_layer(properties, filepath, thumbnail):
    try:
        return(gis.content.add(properties, data = filepath, thumbnail = thumbnail))
    except:
        print('file may already exist')
        search_str = 'title: ' + properties['title']
        search_results = gis.content.search(search_str, 'Feature Layer')
        try:
            return(search_results[0])
        except:
            print('layer not found in gis')


def main():

    ### test logon
    success = False
    attempt = 0 
    while not success and attempt <3:
        success = logon_to_arcgis()
        attempt += 1

    ### testing getting a layer

    csv_path = r"/data/pb_files/CSV/pittsburgh-police-arrest-data_0.csv"
    csv_properties = {'title': 'Pittsburgh Arrest Data',
                 'description':'Pittsburgh Arrest Data downloaded from data.gov',
                 'tags':'arcgis, python, Pittsburgh, arrests, crime'}
    thumbnail_path = r"handcuffs.png"
    
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


if __name__ == '__main__':
    main()
