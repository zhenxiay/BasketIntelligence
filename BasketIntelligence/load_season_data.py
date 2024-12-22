
from pyspark.sql import SparkSession
from google.cloud import bigquery
from BasketIntelligence.create_season import CreateSeason

class LoadSeasonData(CreateSeason):
    def __init__(self, year, project, dataset, table_name):
        super().__init__(year=year)
        self.project = project
        self.dataset = dataset
        self.table_name = table_name

    @staticmethod
    def get_spark():
        spark = SparkSession \
            .builder \
            .appName("BasketIntelligence") \
            .getOrCreate()
        return spark

    @staticmethod
    def create_big_query_client():
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        return client, job_config

    def load_per_game_to_big_query(self):
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        table_id = f'{self.project}.{self.dataset}.{self.table_name}'
        client, job_config = self.create_big_query_client()
        job = client.load_table_from_dataframe(dataset, table_id, job_config=job_config)
        print(f'Data load to big query {table_id} successfully!')

    def load_adv_stats_to_big_query(self):
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        table_id = f'{self.project}.{self.dataset}.{self.table_name}'
        client, job_config = self.create_big_query_client()
        job = client.load_table_from_dataframe(dataset, table_id, job_config=job_config)
        print(f'Data load to big query {table_id} successfully!')
    
    def load_per_game_to_lakehouse(self):
        spark = self.get_spark()
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        dataset_spark = spark.createDataFrame(dataset)

        drop_action = spark.sql(f"DROP TABLE IF EXISTS basketball_reference_per_game_{self.year}")
        print(f"Dropped table basketball_reference_per_game_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_per_game_{self.year}")
        print(f'load per game data of season {self.year} successfully!')
    
    def load_adv_stats_to_lakehouse(self):
        spark = self.get_spark()
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        dataset_spark = spark.createDataFrame(dataset)
        
        drop_action = spark.sql(f"DROP TABLE IF EXISTS basketball_reference_adv_stats_{self.year}")
        print(f"Dropped table basketball_reference_adv_stats_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_adv_stats_{self.year}")
        print(f'load per game data of season {self.year} successfully!')