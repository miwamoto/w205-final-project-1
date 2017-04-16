#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from collections import defaultdict

class PostgreSQL(object):
    """Update database in safely

    This class should be used as a context so that its database
    connection will be released gracefully. If not in a context, the
    class will connect and disconnect on every update or batch of updates.
    """

    def __init__(self, table = None, database = "pittsburgh", user = 'postgres',
                 password = 'postgres', host = 'localhost',
                 port = '5432', cols = None, types = None):

        self.database = database
        self.table    = table
        self.user     = user
        self.password = password
        self.host     = host
        self.port     = port
        self.cols     = cols
        self.types    = types

        self._str_types = {'TEXT', 'CHAR', 'CHARACTER',
                           'CHARACTER VARYING', 'VARCHAR'}

        self.in_context = False
        self.conn       = None
        self.cur        = None


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
        """Open connection to database"""

        if self.conn is None or self.conn.closed == 1:
            self.conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port)

        self.cur = self.conn.cursor()


    def disconnect(self):
        """Close connection to the database"""

        try:
            self.conn.close()
        except AttributeError as e:
            print('Connection may not have been open')


    def create_table(self, table, cols, types):
        """Create table with specified cols and types"""

        self.set_table(table)
        self.cols = cols
        self.types = types

        col_types = ','.join(
            "{} {}".format(col, t) for col, t in zip(self.cols, self.types))

        query = "CREATE TABLE IF NOT EXISTS {}({})".format(
            self.table, col_types)

        self.execute(query)


    def select(self, cols = None, table = None, predicates = None):
        """Select specified columns from given table

        SELECT [cols] FROM [table] [predicates]

        cols       -- array of columns to select (default: *)
        table      -- table to select from (default: last table accessed)
        predicates -- string to add to end of select
        """

        if cols is None:
            cols = "*"
        else:
            cols = ','.join(cols)

        if table is None:
            table = self.table
        else:
            self.set_table(table)

        query = "SELECT {} FROM {} {}".format(cols, table, predicates)

        if self.execute(query):
            return self.cur.fetchall()
        else:
            return []

        
    def set_table(self, table):
        self.table = table


    def str_like(self, s):
        s = s.split('(')[0]
        return s.upper() in self._str_types


    def parse_rows(self, row):
        # If no types specified, treat as strings
        if self.types is not None:
            # Wrap string-like values in quotes
            vals = ','.join("'{}'".format(val)
                            if self.str_like(self.types[i])
                            # Otherwise, send raw values
                            else "{}".format(val)
                            for i, val in enumerate(row))
        else: 
            vals = ','.join("'{}'".format(val) for i, val in enumerate(row))
        return vals



    def add(self, row):
        # If no cols specified, add by position
        if self.cols is not None:
            cols = '({})'.format(','.join(col for col in self.cols))
        else:
            cols = ''

        vals = self.parse_rows(row)

        query = 'INSERT INTO {} {} VALUES({})'.format(self.table, cols, vals)

        return self.execute(query)


    def add_rows(self, rows, types = None, cols = None):
        if types is not None:
            self.types = types
        if cols is not None:
            self.cols = cols

        for row in rows:
            if not self.add(row):
                print('Failed to add row: {}'.format(row))
                print('with header: {}'.format(self.cols))

         
    
    def execute(self, query, fallback_query = None):
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
