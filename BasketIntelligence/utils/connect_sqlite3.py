import sqlite3

def load_multi_stock_data_to_sqlite3(db_path, db_name):        
    return sqlite3.connect(f'{db_path}/{db_name}')