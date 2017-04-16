#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

import configparser

def main(table):
    # So we can read the config file
    parser = configparser.ConfigParser()
    parser.read('/home/ec2-user/credentials.config')

    try:
        database  = parser.get('POSTGRES' , 'database')
        user      = parser.get('POSTGRES' , 'user')
        password  = parser.get('POSTGRES' , 'password')
        host      = parser.get('POSTGRES' , 'host')
        port      = parser.get('POSTGRES' , 'port')
        pittsburgh_db = parser.get('PITTSBURGH'   , 'database')
        # table     = parser.get('PITTSBURGH'   , 'table')

    except configparser.NoSectionError as e:
        print("##############################")
        print("Warning: Improperly formatted config file.")
        print("Please create a file in the ~/ directory")
        print("called `postgres_credentials.config`")
        print("with your desired database configuration details")
        print("and then run `~/setup_database.py`")
        print("##############################")
        print("")
        raise e

    # Forge a strong connection with postgres
    conn = pg.connect(database = database,
                            user     = user,
                            password = password,
                            host     = host,
                            port     = port)

    # Connect to our new database
    conn = pg.connect(database = pittsburgh_db,
                      user     = user,
                      password = password,
                      host     = host,
                      port     = port)


    cur = conn.cursor()

    # And create our table
    try:
        cur.execute('''CREATE TABLE {}
            (word TEXT PRIMARY KEY     NOT NULL,
            count INT     NOT NULL);'''.format(table))
        conn.commit()
    except:
        print("Could not create table {}".format(table))
    finally: # ~~FIN~~ #
        conn.close()
    

if __name__ == '__main__':
    try: 
        table = sys.argv[1]
    except IndexError as e:
        print("Usage: ")
        print("create_table <table_name>")
        exit(1)
        
    main(table)
