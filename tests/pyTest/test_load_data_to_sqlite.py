from BasketIntelligence.load_season_data import LoadSeasonData

def test_load_per_game_to_sqlite():

    loader = LoadSeasonData("2025","project","BasketIntelligence")
    
    table_name = "per_game"
    db_path = "./"
    db_name = "test_db.sqlite"

    loader.load_per_game_to_sqlite(
        table_name, 
        db_path, 
        db_name
    )

def test_dynamic_load_adv_game_sqlite():

    # Initialize the loader
    loader = LoadSeasonData(year="2025", project="project", dataset_name="BasketIntelligence")

    # Load 'adv_stats' data into a SQLite database
    loader.load_data(
        data_source='adv_stats',
        db_type='sqlite',
        table_name='adv_stats',
        db_path='./',
        db_name='test_db.sqlite'
    )