import great_expectations as gx
import pandas as pd
from BasketIntelligence.ml_analysis import k_means_player_clustering

#read data that is to be tested
dataset = k_means_player_clustering(year="2025", 
                                    n_clusters=5)

#create great expectation context and run quality test
context = gx.get_context()

data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": dataset})

expectation_game_count = gx.expectations.ExpectColumnValuesToBeBetween(
    column="kMeans", min_value=0, max_value=4
)
