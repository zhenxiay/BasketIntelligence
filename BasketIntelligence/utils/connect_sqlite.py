'''
Define a module to connect to a SQLite database.
This module is to be used by the `LoadSeasonData` class for data ingestion.

'''
import sqlite3

def connect_sqlite(db_path, db_name):
    '''
    define the function to create & connect to a SQLite database
    '''
    return sqlite3.connect(f'{db_path}/{db_name}')
