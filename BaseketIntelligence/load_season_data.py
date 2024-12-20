
from pyspark.sql import SparkSession

class LoadSeasonData(CreateSeason):
    def __init__(self,year):
        super().__init__(year=year)
        self.year = year

    def get_spark():
        spark = SparkSession \
        .builder \
        .appName("BasketIntelligence") \
        .getOrCreate()
    
    def load_per_game_to_lakehouse(self):
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        dataset_spark = spark.createDataFrame(dataset)

        drop_action = spark.sql(f"DROP TABLE IF EXISTS basketball_reference_per_game_{self.year}")
        print(f"Dropped table basketball_reference_per_game_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_per_game_{self.year}")
        print(f'load per game data of season {self.year} successfully!')
    
    def load_adv_stats_to_lakehouse(self):
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        dataset_spark = spark.createDataFrame(dataset)
        
        drop_action = spark.sql(f"DROP TABLE IF EXISTS basketball_reference_adv_stats_{self.year}")
        print(f"Dropped table basketball_reference_adv_stats_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_adv_stats_{self.year}")
        print(f'load per game data of season {self.year} successfully!')