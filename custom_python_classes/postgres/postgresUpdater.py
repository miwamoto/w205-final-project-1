#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from collections import defaultdict

class PostgreSQL(object):
    """Update database in batches 

    This class should be used as a context so that its database
    connection will be released gracefully. If not in a context, the
    class will connect and disconnect on every update or batch of updates.
    
    When the context is left, the class will dump all values in its
    buffer to the database. The user can manually dump the buffer, as
    well."""

    def __init__(self, table, database = "pittsburgh", user = 'postgres',
                 password = 'postgres', host = 'localhost',
                 port = '5432', cols = None, types = None):

        self.database = database
        self.table = table
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.cols = cols
        self.types = types

        self.in_context = False
        self.conn = None
        self.cur = None


    def __enter__(self):
        """Open psql connection when entering context"""
        self.in_context = True
        self.connect()
        return self


    def __exit__(self, *args):
        """Safely close psql connection when exiting context"""
        try:
            self.dump_buffer()
        finally:
            self.conn.close()
        return False


    def reset(self, word):
        self.conn.rollback()


    def dump_buffer(self):
        pass


    def connect(self):
        if self.conn is None or self.conn.closed == 1:
            self.conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port)

        self.cur = self.conn.cursor()


    def disconnect(self):
        self.conn.close()


    def create_table(self, table, cols, types):
        self.table = table
        self.cols = cols
        self.types = types

        col_types = ','.join(
            "{} {}".format(col, t) for col, t in zip(self.cols, self.types))
        query = "CREATE TABLE IF NOT EXISTS {}({})".format(
            self.table, col_types)


    def add(self, row):
        # If no cols specified, add by position
        if self.cols is not None:
            cols = '({})'.format(','.join(col for col in self.cols))
        else:
            cols = ''

        # If no types specified, treat as strings
        if self.types is not None:
            vals = ','.join("'{}'".format(val) if self.types[i] == 'str'
                            else "{}".format(val)
                            for i, val in enumerate(vals))
        else: 
            vals = ','.join("'{}'".format(val) for i, val in enumerate(vals))

        query = 'INSERT INTO {} {} VALUES({})'.format(self.table, cols, vals)
        print(query)

        # return self.execute(query)


    def add_rows(self, rows, types = None, cols = None):
        if types is not None:
            self.types = types
        if cols is not None:
            self.cols = cols

        for row in rows:
            if not self.add(row):
                print('Failed to add row: {}'.format(row))
                print('with header: {}'.format(self.cols))

         
    
    def execute(self, query, fallback_query):
        """Execute query. Execute fallback on failure"""

        try:
            self.cur.execute(query)
            self.conn.commit()
            return True
        except:
            self.conn.rollback()
        
        if fallback_query is not None:
            try:
                self.cur.execute(fallback_query)
                self.conn.commit()
                return True
            except:
                self.conn.rollback()
                return False
        else:
            return False


if __name__ == "__main__":
    pass
