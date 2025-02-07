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

expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="G", min_value=0, max_value=82,
    column="FG_pct", min_value=0, max_value=1,
    column="2P_pct", min_value=0, max_value=1,
    column="FT_pct", min_value=0, max_value=1,
    column="3P_pct", min_value=0, max_value=1
)

validation_result = batch.validate(expectation)

print(validation_result)
