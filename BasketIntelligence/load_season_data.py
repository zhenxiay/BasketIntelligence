
from pyspark.sql import SparkSession
from google.cloud import bigquery

class LoadSeasonData(create_season):
    def __init__(self,year):
        super().__init__(year=year)
        self.year = year
        self.project = project
        self.dataset = dataset
        self.table_name = table_name

    def get_spark():
        spark = SparkSession \
        .builder \
        .appName("BasketIntelligence") \
        .getOrCreate()

    def create_big_query_client():
        # Construct a BigQuery client object.
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # WRITE_APPEND, WRITE_EMPTY
        return client, job_config

    def load_per_game_to_big_uqery(self):
        dataset = create_season.CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        table_id = f'{self.project}.{self.datast}.{self.table_name}'
        client, job_config = create_big_query_client()
        job = client.load_table_from_dataframe(dataset, table_id, job_config=job_config)
        print('Data load to big query successfully!')

    def load_adv_stats_to_big_uqery(self):
        dataset = create_season.CreateSeason(self.year).read_adv_stats_game().drop(columns=['Awards'])
        table_id = f'{self.project}.{self.datast}.{self.table_name}'
        client, job_config = create_big_query_client()
        job = client.load_table_from_dataframe(dataset, table_id, job_config=job_config)
        print('Data load to big query successfully!')
    
    def load_per_game_to_lakehouse(self):
        spark = self.get_spark()
        dataset = create_season.CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        dataset_spark = spark.createDataFrame(dataset)

        drop_action = spark.sql(f"DROP TABLE IF EXISTS basketball_reference_per_game_{self.year}")
        print(f"Dropped table basketball_reference_per_game_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_per_game_{self.year}")
        print(f'load per game data of season {self.year} successfully!')
    
    def load_adv_stats_to_lakehouse(self):
        spark = self.get_spark()
        dataset = create_season.CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        dataset_spark = spark.createDataFrame(dataset)
        
        drop_action = spark.sql(f"DROP TABLE IF EXISTS basketball_reference_adv_stats_{self.year}")
        print(f"Dropped table basketball_reference_adv_stats_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_adv_stats_{self.year}")
        print(f'load per game data of season {self.year} successfully!')