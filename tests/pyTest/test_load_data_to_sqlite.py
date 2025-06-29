from BasketIntelligence.load_season_data import load_per_game_to_sqlite

def test_load_per_game_to_sqlite():
    
	assert load_per_game_to_sqlite() is true