from pyspark.sql import SparkSession

def get_spark():
    '''
    Create a Spark session for data processing.
    '''
    spark = SparkSession \
            .builder \
            .appName("BasketIntelligence") \
            .getOrCreate()
    return spark