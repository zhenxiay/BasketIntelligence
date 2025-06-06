from BasketIntelligence.ml_analysis import k_means_team_shooting_clustering, k_means_player_clustering
from BasketIntelligence.create_season import CreateSeason

def test_k_means_team_shooting():
  
	dataset = k_means_team_shooting_clustering(year="2025", 
                                             n_clusters=5)
	columns_count = len(dataset.columns)
	assert columns_count == 2

def test_k_means_player():
  
	dataset = k_means_player_clustering(year="2025", 
                                      n_clusters=5)
	columns_count = len(dataset.columns)
	assert columns_count == 6
