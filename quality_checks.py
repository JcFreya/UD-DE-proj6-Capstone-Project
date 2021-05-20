# All imports and installs
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

import pandas as pd
import configparser
import datetime as dt

import utils

# Spark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear, upper
from pyspark.sql.functions import monotonically_increasing_id


def loading_checks(input_data, table_name):
    """Check the count of rows of fact and dimension tables to ensure the table is loaded as expected
    
    Args:
        input_data: spark dataframe to check counts on
        table_name: corresponding name of table
    """
    total_count = input_data.count()

    if total_count == 0:
        print(f"Data quality check failed for {table_name} with zero records!")
    else:
        print(f"Data quality check passed for {table_name} with {total_count:,} records.")
    return 0


def count_checks(source_table, loaded_table):
    """Check the count of rows of fact and dimension tables the same as source table
    
    Args:
        source_table: spark dataframe to check counts on
        loaded_table: corresponding name of table
    """
    source_table_count = source_table.count()
    table_count = loaded_table.count()

    if source_table_count == table_count:
        print(f"Data quality check failed for loaded table has data missing while trasfering from source data")
    else:
        print(f"Data quality check passed for loaded table has same number of rows as source data")
    return 0
   
