from BasketIntelligence.create_season import CreateSeason

def test_read_team_adv_stats():
  
	dataset = CreateSeason("2025")
	columns_count = len(dataset.read_team_adv_stats().columns)
	assert columns_count == 29

def test_read_stats_per_game():
    
		dataset = CreateSeason("2025")
		columns_count = len(dataset.read_stats_per_game().columns)
		assert columns_count == 32

def test_read_team_shooting():
		dataset = CreateSeason("2025")
		columns_count = len(dataset.read_team_shooting().columns)
		assert columns_count == 16
