from sqlalchemy import create_engine

def connect_postgres(user, pwd, host, db):
    '''
    define the function to create an engine to a postgres database.
    '''
    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:5432/{db}')
    return engine
    