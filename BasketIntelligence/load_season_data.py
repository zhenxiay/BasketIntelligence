
from pyspark.sql import SparkSession
from google.cloud import bigquery
from BasketIntelligence.create_season import CreateSeason

class LoadSeasonData(CreateSeason):
    def __init__(self, year, project, dataset_name):
        super().__init__(year=year)
        self.project = project
        self.dataset_name = dataset_name

    @staticmethod
    def get_spark():
        spark = SparkSession \
            .builder \
            .appName("BasketIntelligence") \
            .getOrCreate()
        return spark

    @staticmethod
    def data_ingestion_lakehouse(dataset,name): -> None
        spark = self.get_spark()
        dataset_spark = spark.createDataFrame(dataset)
        
        spark.sql(f"DROP TABLE IF EXISTS basketball_reference_{name}_{self.year}")
        print(f"Dropped table basketball_reference_{name}_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_{name}_{self.year}")
        print(f'load table basketball_reference_{name}_{self.year} successfully!')        

    @staticmethod
    def create_big_query_client():
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        return client, job_config

    @staticmethod
    def data_ingestion_big_query(dataset,table_name): -> None
        table_id = f'{self.project}.{self.dataset_name}.{table_name}'
        client, job_config = self.create_big_query_client()
        client.load_table_from_dataframe(dataset, table_id, job_config=job_config)
        print(f'Data load to big query {table_id} successfully!')

    def load_per_game_to_big_query(self,table_name): -> None
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        self.data_ingestion_big_query(dataset,table_name)

    def load_adv_stats_to_big_query(self,table_name): -> None
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        data_ingestion_big_query(dataset,table_name)

    def load_team_adv_stats_to_big_query(self,table_name): -> None
        dataset = CreateSeason(self.year).read_team_adv_stats()
        data_ingestion_big_query(dataset,table_name)
        
    def load_team_shooting_to_big_query(self,table_name): -> None
        dataset = CreateSeason(self.year).read_team_shooting()
        data_ingestion_big_query(dataset,table_name)
    
    def load_per_game_to_lakehouse(self): -> None
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        self.data_ingestion_lakehouse(dataset,'per_game')
    
    def load_adv_stats_to_lakehouse(self): -> None
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        self.data_ingestion_lakehouse(dataset,'adv_stats')

    def load_team_adv_stats_to_lakehouse(self): -> None
        dataset = CreateSeason(self.year).read_team_adv_stats()
        self.data_ingestion_lakehouse(dataset,'team_adv_stats')
        
    def load_team_shooting_to_lakehouse(self): -> None
        dataset = CreateSeason(self.year).read_team_shooting()
        self.data_ingestion_lakehouse(dataset,'team_shooting')
