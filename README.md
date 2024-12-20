#### Add year as an argument to retrieve the data from a season
dataset = CreateSeason("2025")
#### Use method to load per game data or adv stats
dataset.read_stats_per_game()

dataset.read_adv_stats()
