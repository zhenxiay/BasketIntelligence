def get_spark():
    '''
    Create a Spark session for data processing.
    '''
    from pyspark.sql import SparkSession

    spark = SparkSession \
            .builder \
            .appName("BasketIntelligence") \
            .getOrCreate()
    return spark
    