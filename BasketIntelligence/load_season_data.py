
from pyspark.sql import SparkSession
from google.cloud import bigquery
from sqlalchemy import create_engine
from BasketIntelligence.create_season import CreateSeason

class LoadSeasonData(CreateSeason):
    def __init__(self, year, project, dataset_name):
        super().__init__(year=year)
        self.project = project
        self.dataset_name = dataset_name

############ database setups for postgres SQL ################################

    @staticmethod
    def connect_postgres(user, pwd, host, db):
        engine = create_engine(f'postgresql://{user}:{pwd}@{host}:5432/{db}')
        return engine
    
    def data_ingestion_postgres(self,dataset,table_name,user,pwd,host,db) -> None:
        engine = self.connect_postgres(user, pwd, host, db)
        engine.connect()

        dataset.to_sql(table_name,
                       con=engine,
                       if_exists="replace")

        print(f'{table_name} load to postgres database {host}/{db} successfully!')

############ database setups for MS fabric lakehouse ######################

    @staticmethod
    def get_spark():
        spark = SparkSession \
            .builder \
            .appName("BasketIntelligence") \
            .getOrCreate()
        return spark

    def data_ingestion_lakehouse(self,dataset,name) -> None:
        spark = self.get_spark()
        dataset_spark = spark.createDataFrame(dataset)
        
        spark.sql(f"DROP TABLE IF EXISTS basketball_reference_{name}_{self.year}")
        print(f"Dropped table basketball_reference_{name}_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_{name}_{self.year}")
        print(f'load table basketball_reference_{name}_{self.year} successfully!')

############ database setups for big query ###############

    @staticmethod
    def create_big_query_client():
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        return client, job_config

    def data_ingestion_big_query(self,dataset,table_name) -> None:
        table_id = f'{self.project}.{self.dataset_name}.{table_name}'
        client, job_config = self.create_big_query_client()
        client.load_table_from_dataframe(dataset, table_id, job_config=job_config)
        print(f'Data load to big query {table_id} successfully!')

############ Methods for loading data into postgres SQL ##########################

    def load_per_game_to_postgres(self,dataset,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_adv_stats_to_postgres(self,dataset,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_team_adv_stats_to_postgres(self,dataset,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_team_adv_stats()
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_team_shooting_to_postgres(self,dataset,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_team_shooting()
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

############ Methods for loading data into bigquery ###############################

    def load_per_game_to_big_query(self,table_name) -> None:
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        self.data_ingestion_big_query(dataset,table_name)

    def load_adv_stats_to_big_query(self,table_name) -> None:
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        self.data_ingestion_big_query(dataset,table_name)

    def load_team_adv_stats_to_big_query(self,table_name) -> None:
        dataset = CreateSeason(self.year).read_team_adv_stats()
        self.data_ingestion_big_query(dataset,table_name)
        
    def load_team_shooting_to_big_query(self,table_name) -> None:
        dataset = CreateSeason(self.year).read_team_shooting()
        self.data_ingestion_big_query(dataset,table_name)

############ Methods for loading data into fabric lakehose ############################
    
    def load_per_game_to_lakehouse(self) -> None:
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        self.data_ingestion_lakehouse(dataset,'per_game')
    
    def load_adv_stats_to_lakehouse(self) -> None:
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        self.data_ingestion_lakehouse(dataset,'adv_stats')

    def load_team_adv_stats_to_lakehouse(self) -> None:
        dataset = CreateSeason(self.year).read_team_adv_stats()
        self.data_ingestion_lakehouse(dataset,'team_adv_stats')
        
    def load_team_shooting_to_lakehouse(self) -> None:
        dataset = CreateSeason(self.year).read_team_shooting()
        self.data_ingestion_lakehouse(dataset,'team_shooting')
