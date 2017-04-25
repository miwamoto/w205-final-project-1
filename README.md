# w205 Final Project

## Setup

This project requires a custom AMI with pre-installed software. Please check with Zach Ingbretsen to receive the AMI if you do not already have access.

### Git
Please run this on every new AMI version
```bash
git config --global user.name "Your Name"
git config --global user.email "your_email@xyz.com"
git pull
```

### Mounting volume
You must mount an EBS volume to store the Postgres database. If you are using the same volume that you used for class, you can just run:
```bash
startup
```
to mount the drive (assuming it's in the default location of /dev/xvdf). If this is the initial setup, it will give a warning about directory /data/pittsburgh_db/data which will be created in the next step. Please proceed.

If you are using a new volume, run the following to format the drive at the given location and then mount it:
```bash
sudo su
sh /home/ec2-user/custom/bin/wipe_drive /dev/xvdf
exit
```

### Postgres

In order to set up the database, run:
```bash
sudo su
/home/ec2-user/custom/bin/setup_pitts_db
exit
```

This will create a new postgres database and start the server. In the future to start the server, just run:
```bash
start_postgres
```

Likewise, to stop the server, run:
```bash
stop_postgres
```

## Usage

### Postgres

#### psql
You can use postgres as normal:
```bash
psql -U postgres pittsburgh
```

This will bring you to the command prompt for the database `pittsburgh`. Type `\q` to exit.

#### psycopg2
psycopg2 and sqlalchemy are both installed, and can be used in our Python scripts. However, for simple operations you can use the following:

#### custom package postgres
In the custom_python_classes directory is the package `postgres` with one module `PostgreSQL.py` with the class `PostgreSQL`.

This can be used in python scripts by importing the class, and using it with the `with` context. For example:
```python
from postgres.PostgreSQL import PostgreSQL
# Define your column names
COLS = [ 'Name', 'Number' ]

# Define the types for each column
TYPES = ['VARCHAR', 'INT']

# With a connection to the database, create a table of COLS and TYPES
with PostgreSQL(database = 'pittsburgh') as psql:
    psql.create_table(table = 'test', cols = COLS, types = TYPES)

# Some toy data (that matches the TYPES)
rows = [['Jason', 1], ['Mona', 2], ['Richard', 3], ['Zach', 4]]

# With a connection to a table in the  database,
# add the rows of proper cols and TYPES
with PostgreSQL(table = 'test', database = 'pittsburgh') as psql:
    psql.add_rows(rows, cols = COLS, types = TYPES)
```

## Data Ingestion

### OpenWeatherMap, Wunderground, Poverty, and Data.gov

Make sure Postgres server is already running!
```bash
start_postgres
```

The ingestion layer retrieves several raw datasets and makes them available for the user: openweathermap (weather predictions), wunderground (weather history), download_gov_data (pittsburgh crime data), and poverty (Pittsburgh neighborhood poverty).

The following command will pull down all data from several data sources and store them in the `pittsburgh` database. The scripts do incremental updates when possible. For example, the script to pull historical weather data will only pull new data after it is run once.

The data.gov sources are tracked by their metadata identifiers. The first time running this command will download all data, and store their metadata in the table `metatable` in the `pittsburgh` database. Each subsequent time the batch layer is run, the new metadata is compared against the metadata stored in `metatable` to see if new data is available. If so, it will pull the new data, and ensure there are no duplicates in the table. If the metadata matches what was previously downloaded, that dataset will not be downloaded again.

When possible, separate files from a given dataset are combined into one table (and all duplicates are discarded). If the data are not compatible, they are stored in separate tables. Data are *NOT* type checked upon download. All data are stored in the initial table as TEXT values only.

Run this command to download all data for the analysis:
```bash
run_ingestion_layer
```

That command runs all of the following (i.e. there is NO need to run anything further for the *Data Ingestion* section):

This file (called by `run_ingestion_layer`) is responsible for retrieving the data from data.gov and managing the `metatable`:
```bash
cd pulling_data
python download_gov_data.py
```

This has been setup to run with a whitelist of data; the full dataset includes an extremely large volume of unrelated data. If the user wishes to change the data available, they can easily modify the whitelist. download_gov_data.py will pull the data and store it in the pittsburgh database in postgres. Typical run times are around 20 minutes.

Similarly, `run_batch_layer` will also pull down the supporting data sets. The poverty dataset and the weather forecasts are small downloads, and we want the current data, so we download these every thing the batch layer runs, and replace the prior tables. As previously mentioned, the Weather Underground script (wunderground.py) does incremental updates.
```bash
python openweathermap.py
python wunderground.py
python poverty.py
```

These will all be used for additional follow-on analysis. Historical weather data is from 1990-2017. Weather forecasts are for the five day outlook for predictive analysis. Poverty data for pittsburgh will also be used in the analysis.

#### Confirm Data Retrieval

Connect to the data base and perform a few lookups (recommended to be performed in another command window):
```
psql -U postgres pittsburgh
\dt
select * from police_incident_blotter_archive_0 limit 10;
```
Similarly, the user will be prompted to test the other retrieval program successes.

## Batch Processing

For updating the data, this can be done in batches. Importantly, before running forecasts, be sure to run the batch update for the latest data. This will retrieve updated weather forecasts (which are necessary for predicting the next 5 days).

Run:
```bash
run_batch_layer
```

In particular, this runs the following batch processes to collect data updates, clean the raw data, create live crime incidence forecasts and run ArcGIS analytics. There is NO need to run any of the code in the remainder of the *Batch Processing* section it is for explanatory purposes.

The batch layer first re-runs openweathermap.py from the ingestion layer because the forecasts are important for the predictions.
```
cd ~/pulling_data
python openweathermap.py
```

Next the batch layer runs a command that selects tables of interest, and processes them into a usable form. As previously mentioned, the raw data are stored as TEXT only. This stage does type-checking and conversions in a pseudo-schema-on-read manner. The data stored upstream are relatively unconstrained.

In this stage, we convert numerical values to numerical datatypes. This includes strings that include dollar signs, percentages, and commas, which are not automatically parsed as numbers by pandas. When possible, it splits dates and times into their component parts. Column names are standardized (as much as possible) and particularly obscure ones are converted into human-readable form. Some of this was automated, but much of it was hand-coded to catch specific edge cases.
```
cd ~/batch_processing
python batch.py
```

### Machine learned crime incidence forecasts

Crime forecasts will be of interest for law enforcement and city officials. This is just a first step in the development of the classifier, but it endeavors to predict the probability of crime occurring in different neighborhoods in the city of Pittsburgh.
```
python createforecasts.py
```

As well as posting to the database this will also create forecasts.csv which will be located in /data/forecasts for use in later analysis.


### Geographic Information System Components (GIS)

To process the data from the Pittsburgh database, run
```
python arcGISinterface.py
```

This process ingests the data and creates resources known as layers and datasets that can be used for geospatial analysis.  These components are stored in an online respository (ESRI ArcGIS online). Further, these resources are published so that they become part of a large searchable repository of GIS features and maps.


## Serving Layer

### Jupyter Notebooks

There is one jupyter notebooks available for GIS.

The GIS notebook, `Interacting_with_Pittsburgh_Data_with_Jupyter_and_ArcGIS.ipynb` demonstrates how the GIS resources created in the process above can be used for analysis with Python and its associated tools such as pandas, NumPy and SciPy. 

You can read more about this API for Python at `https://developers.arcgis.com/python/` To view the example notebook, start Jupyter notebook as indicated below.

```
cd gis_ipynb_example 
jupyter-notebook
```
You can now access your notebooks over the web via your instance's public DNS address, at the port 2017 (e.g., ec2-
XX-XXX-XX-XXX.compute-1.amazonaws.com:2017)


### HTML
The server is currently set up to run CherryPy, a simple Python webserver. To start the webserver, run:
```
cd html_example
python main.py
```
You can now access the website via your instance's public DNS address, at the port 8080 (e.g., ec2-XX-XXX-XX-XXX.co
mpute-1.amazonaws.com:8080)
