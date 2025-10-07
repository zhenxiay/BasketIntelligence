
from BasketIntelligence.create_season import CreateSeason
from BasketIntelligence.ml_analysis import k_means_team_shooting_clustering, k_means_player_clustering
from BasketIntelligence.utils.logger import get_logger
from BasketIntelligence.utils.connect_sqlite import connect_sqlite
from BasketIntelligence.utils.connect_postgres import connect_postgres
from BasketIntelligence.utils.create_big_query_client import create_big_query_client
from BasketIntelligence.utils.create_spark_session import get_spark

class LoadSeasonData(CreateSeason):
    '''
    Create a class to enable data ingestion to different databases.
    '''
    def __init__(self, year, project, dataset_name):
        super().__init__(year=year)
        self.project = project
        self.dataset_name = dataset_name
        self.logger = get_logger()

        self._DATA_SOURCES = {
            'per_game': lambda: self.read_stats_per_game().drop(columns=['Awards']),
            'adv_stats': lambda: self.read_adv_stats().drop(columns=['Awards']),
            'team_adv_stats': self.read_team_adv_stats,
            'team_shooting': self.read_team_shooting,
            'kmeans_team_shooting': k_means_team_shooting_clustering,
            'kmeans_player': k_means_player_clustering,
        }

        self._DB_INGESTION_MAPPING = {
            'postgres': self.data_ingestion_postgres,
            'sqlite': self.data_ingestion_sqlite,
            'big_query': self.data_ingestion_big_query,
            'fabric_lakehouse': self.data_ingestion_lakehouse,
            'unity_catalog': self.data_ingestion_lakehouse,
        }

############ database setups for postgres SQL ################################

    def data_ingestion_postgres(self,dataset,table_name,user,pwd,host,db) -> None:
        '''
        define the function to load data into a postgres database.
        This function is to be used by the methods below.
        '''
        engine = connect_postgres(user, pwd, host, db)
        engine.connect()

        dataset.to_sql(table_name,
                       con=engine,
                       if_exists="replace")

        self.logger.info(f'{table_name} load to postgres database {host}/{db} successfully!')

############ database setups for sqlite3 ##################################

    def data_ingestion_sqlite(self,dataset,table_name,db_path,db_name) -> None:
        '''
        define the function to load data into a sqlite database.
        This function is to be used by the methods below.
        '''
        engine = connect_sqlite(db_path,db_name)

        dataset.to_sql(table_name,
                       con=engine,
                       if_exists="replace")

        self.logger.info(f'{table_name} loaded to sqlite db {db_path}/{db_name} successfully!')
        self.logger.info(f'Rows count: {len(dataset)}')

############ database setups for MS fabric lakehouse ######################

    def data_ingestion_lakehouse(self,dataset,table_name) -> None:
        '''
        define the function to load data into a MS fabric lakehouse.
        This function is to be used by the methods below.
        '''
        spark = get_spark()
        self.logger.info("Spark session initialized...")
        dataset_spark = spark.createDataFrame(dataset)

        spark.sql(f"DROP TABLE IF EXISTS basketball_reference_{table_name}_{self.year}")
        self.logger.info(f"Dropped table basketball_reference_{table_name}_{self.year} in the lakehouse...")

        dataset_spark.write.saveAsTable(f"basketball_reference_{table_name}_{self.year}")
        self.logger.info(f'load table basketball_reference_{table_name}_{self.year} successfully!')

############ database setups for big query ###############

    def data_ingestion_big_query(self,dataset,table_name) -> None:
        '''
        define the function to load data into a google big query database.
        This function is to be used by the methods below.
        '''
        table_id = f'{self.project}.{self.dataset_name}.{table_name}'
        client, job_config = create_big_query_client()
        client.load_table_from_dataframe(dataset, table_id, job_config=job_config)
        self.logger.info(f'Data load to big query {table_id} successfully!')

############ define a dynamic data load method ################

    def load_data(self, data_source: str, db_type: str, **kwargs):
        """
        Loads data from a specified source to a specified database type.

        :param data_source: The name of the data source to load.
                            Options: 'per_game', 'adv_stats', 'team_adv_stats',
                                     'team_shooting', 'kmeans_team_shooting', 'kmeans_player'.
        :param db_type: The type of database to load the data into.
                        Options: 'postgres', 'sqlite', 'big_query', 'lakehouse'.
        :param kwargs: Database-specific connection parameters.
                       - for postgres: table_name, user, pwd, host, db
                       - for sqlite: table_name, db_path, db_name
                       - for big_query: table_name
                       - for lakehouse/ unity catalog: table_name (used as part of the table name)
                       - for kmeans sources: n_cluster
        """
        if data_source not in self._DATA_SOURCES:
            raise ValueError(f"Invalid data source: {data_source}")
        if db_type not in self._DB_INGESTION_MAPPING:
            raise ValueError(f"Invalid db type: {db_type}")

        self.logger.info(f"Loading data for '{data_source}' into '{db_type}'")

        # Get the dataset
        data_func = self._DATA_SOURCES[data_source]
        if 'kmeans' in data_source:
            if 'n_cluster' not in kwargs:
                raise ValueError("n_cluster is required for kmeans data sources")
            dataset = data_func(self.year, kwargs.pop('n_cluster'))
        else:
            dataset = data_func()

        # Get the ingestion function and load data
        ingestion_func = self._DB_INGESTION_MAPPING[db_type]
        ingestion_func(dataset, **kwargs)

        self.logger.info(f"Data loaded for '{data_source}' into '{db_type}' successfully!")
        self.logger.info(f"Rows count: {len(data_source)}")

############ Methods for loading data into postgres SQL ##########################

    def load_per_game_to_postgres(self,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_adv_stats_to_postgres(self,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_team_adv_stats_to_postgres(self,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_team_adv_stats()
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_team_shooting_to_postgres(self,table_name,user,pwd,host,db) -> None:
        dataset = CreateSeason(self.year).read_team_shooting()
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_kmeans_team_shooting_to_postgres(self,table_name,n_cluster,user,pwd,host,db) -> None:
        dataset = k_means_team_shooting_clustering(self.year,n_cluster)
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

    def load_kmeans_player_to_postgres(self,table_name,n_cluster,user,pwd,host,db) -> None:
        dataset = k_means_player_clustering(self.year,n_cluster)
        self.data_ingestion_postgres(dataset,table_name,user,pwd,host,db)

############ Methods for loading data into sqlite database ##########################

    def load_per_game_to_sqlite(self,table_name,db_path,db_name) -> None:
        dataset = CreateSeason(self.year).read_stats_per_game().drop(columns=['Awards'])
        self.data_ingestion_sqlite(dataset,table_name,db_path,db_name)

    def load_adv_stats_to_sqlite(self,table_name,db_path,db_name) -> None:
        dataset = CreateSeason(self.year).read_adv_stats().drop(columns=['Awards'])
        self.data_ingestion_sqlite(dataset,table_name,db_path,db_name)

    def load_team_adv_stats_to_sqlite(self,table_name,db_path,db_name) -> None:
        dataset = CreateSeason(self.year).read_team_adv_stats()
        self.data_ingestion_sqlite(dataset,table_name,db_path,db_name)

    def load_team_shooting_to_sqlite(self,table_name,db_path,db_name) -> None:
        dataset = CreateSeason(self.year).read_team_shooting()
        self.data_ingestion_sqlite(dataset,table_name,db_path,db_name)

    def load_kmeans_team_shooting_to_sqlite(self,table_name,n_cluster, db_path,db_name) -> None:
        dataset = k_means_team_shooting_clustering(self.year,n_cluster)
        self.data_ingestion_sqlite(dataset,table_name,db_path,db_name)

    def load_kmeans_player_to_sqlite(self,table_name,n_cluster,db_path,db_name) -> None:
        dataset = k_means_player_clustering(self.year,n_cluster)
        self.data_ingestion_sqlite(dataset,table_name,db_path,db_name)

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

    def load_kmeans_team_shooting_to_big_query(self,table_name,n_cluster) -> None:
        dataset = k_means_team_shooting_clustering(self.year,n_cluster)
        self.data_ingestion_big_query(dataset,table_name)

    def load_kmeans_player_to_big_query(self,table_name,n_cluster) -> None:
        dataset = k_means_player_clustering(self.year,n_cluster)
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

    def load_kmeans_team_shooting_to_lakehouse(self,table_name,n_cluster) -> None:
        dataset = k_means_team_shooting_clustering(self.year,n_cluster)
        self.data_ingestion_lakehouse(dataset,table_name)

    def load_kmeans_player_to_lakehouse(self,table_name,n_cluster) -> None:
        dataset = k_means_player_clustering(self.year,n_cluster)
        self.data_ingestion_lakehouse(dataset,table_name)
