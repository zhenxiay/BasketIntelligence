from google.cloud import bigquery

def create_big_query_client():
    '''
    Define a function to create a BigQuery client and job configuration.
    '''
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    return client, job_config
    