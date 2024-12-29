#### Link to an example dashboard (loop studio):
https://lookerstudio.google.com/reporting/10cd2c10-17f3-4e0e-aa9c-01fb6470516e/page/x05ZE

#### How to import library
from BasketIntelligence.create_season import CreateSeason

#### Add year as an argument to retrieve the data from a season
dataset = CreateSeason("2025")
#### Use method to read per game data or adv stats
dataset.read_stats_per_game()
dataset.read_adv_stats()
dataset.read_team_adv_stats()

 ### Method to load data
 Currently the libaray offers API to load data to Google big query or to MS Fabric lakehouse:
 ##### Create a dataset that is to be loaded with pollowing parameters:
 year, big query project id, dataset id and table id
 dataset = LoadSeasonData("2025","keen-vial-420113","BasketIntelligence")
 ##### Load to big query:
 dataset.load_per_game_to_big_query("per_game_stats")
 ##### Load to MS fabric lakehouse:
 datasetload_adv_stats_to_lakehouse()
