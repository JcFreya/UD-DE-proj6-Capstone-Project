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


def creat_spark_session():
    """
    Create and return spark session

    Args:
        None

    Returns:
        spark: Spark Session

    """
    spark = SparkSession.builder.getOrCreate()
    return spark


def check_missing_values(df):
    """Check the missing values in dataframe
    
    Args:
        df: dataframe the data to check
    
    Returns:
        df_missing: dataframe showing the missing values 
    """
    
    nrows = df.count()
    
    # get missing value count for each column
    df_missing = df.select([(count(when(isnan(c) | col(c).isNull(), c))/nrows).alias(c) for c in df.columns]).toPandas()

    # format the missing value info
    df_missing = pd.melt(df_missing, var_name='cols', value_name='values')
    
    return df_missing
    

def get_cols_to_drop(df_missing, pct):
    """return the cols to drop based on missing value percentage
    
    Args:
        df_missing: dataframe showing the missing values 
        pct: the percentage to decide which col to drop
    
    Returns:
        drop_cols: columns names to drop
    """
    drop_cols = list(df_missing[df_missing['values']>pct]['cols'])
    
    return drop_cols
    


