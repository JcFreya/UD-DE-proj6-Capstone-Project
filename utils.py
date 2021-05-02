from pyspark.sql import SparkSession

def creat_spark_session():
    """
    Create and return spark session

    Args:
        None

    Returns:
        spark: Spark Session

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark

    