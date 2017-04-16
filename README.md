# w205 Final Project

## Setup

### Git
Please run this on every new AMI version
```bash
git config --global user.name "Your Name"
git config --global user.email "your_email@xyz.com"
```

### Mounting volume
You must mount an EBS volume to store the Postgres database. If you are using the same volume that you used for class, you can just run:
```
startup
```
to mount the drive (assuming it's in the default location of /dev/xvdf).

If you are using a new volume, run the following to format the drive at the given location:
```
wipe_drive /dev/xvdf
startup
```

### Postgres

In order to set up the database, run:
```
sudo su
setup_pitts_db
exit
```

This will create a new postgres database and start the server. In the future to start the server, just run:
```
start_postgres
```

Likewise, to stop the server, run:
```
stop_postgres
```

## Usage

### Postgres

#### psql
You can use postgres as normal:
```
psql -U postgres pittsburgh
```

This will bring you to the command prompt for the database `pittsburgh`

#### psycopg2
psycopg2 is installed, and can be used in our Python scripts. However, for simple operations I would recommend using the following:

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

You can see a more complete example in pulling_data/wunderground.py

### Jupyter Notebooks
```
cd gis_ipynb_example # or wherever your notebooks are
jupyter-notebook
```
You can now access your notebooks over the web via your instance's public DNS address, at the port 2017 (e.g., ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com:2017)


### HTML
The server is currently set up to run CherryPy, a simple Python webserver. To start the webserver, run:
```
cd html_example
python main.py
```
You can now access the website via your instance's public DNS address, at the port 8080 (e.g., ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com:8080)