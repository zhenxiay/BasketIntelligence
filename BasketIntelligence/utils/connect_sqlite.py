import sqlite3

def connect_sqlite(db_path, db_name):        
    return sqlite3.connect(f'{db_path}/{db_name}')