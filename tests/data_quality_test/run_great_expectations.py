import great_expectations as gx
import pandas as pd
from BasketIntelligence.create_season import CreateSeason

#read data that is to be tested
dataset = CreateSeason("2025")

df = dataset.read_stats_per_game()

#create great expectation context and run quality test
context = gx.get_context()

data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

expectation_game_count = gx.expectations.ExpectColumnValuesToBeBetween(
    column="G", min_value=0, max_value=82
)

expectation_FG_pct = gx.expectations.ExpectColumnValuesToBeBetween(
    column="FG_pct", min_value=0, max_value=1
)

for e in [expectation_game_count, expectation_FG_pct]:
    validation_result = batch.validate(e)
    print(validation_result)
