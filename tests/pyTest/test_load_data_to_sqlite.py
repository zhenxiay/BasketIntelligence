from BasketIntelligence.load_season_data import LoadSeasonData

def test_load_per_game_to_sqlite():

    dataset = LoadSeasonData("2025","project","BasketIntelligence")
    
    table_name = "per_game"
    db_path = "./"
    db_name = "test_db"
    
	assert dataset.load_per_game_to_sqlite(table_name, db_path, db_name) is true