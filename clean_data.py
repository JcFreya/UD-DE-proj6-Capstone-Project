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

def clean_immigration_data(input_data):
    """Clean immigration dataframe
    
    Args:
        input_data: spark dataframe with monthly immigration data
    
    Returns:
        df_immigration_clean: cleaned dataframe
    """
    
    df_immigration_selected = input_data[['cicid', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate',
                                          'i94mode', 'i94addr', 'depdate', 'i94bir', 'visatype',
                                          'count', 'visapost', 'occup', 'gender', 'airline']]
    # check missing values
    df_missing = utils.check_missing_values(df_immigration_selected)
    
    # get columns to drop which have missing values over 50%
    drop_cols = utils.get_cols_to_drop(df_missing, 0.5)
    
    # drop the columns which have missing data > 50% since it won't be helpful for analysis
    df_immigration_clean = input_data.drop(*drop_cols)
    
    # drop rows where all elements are missing
    df_immigration_clean = df_immigration_clean.dropna(how='all')
    
    # drop duplicates if exists
    df_immigration_clean = df_immigration_clean.drop_duplicates()

    return df_immigration_clean


def clean_temperature_data(input_data):
    """Clean temperatures dataset
    
    Args:
        input_data: dataframe immigration dataframe
    
    Returns:
        df_temperature: spark dataframe arrive date dimension table
    """
    
    # convert dt column type to string
    df_temperature = input_data.withColumn("dt",col("dt").cast(StringType())) 
    
    # convert Country column to upper case
    df_temperature = df_temperature.withColumn("Country",upper(col("Country"))) 

    # drop rows with missing average temperature
    df_temperature_clean = df_temperature.dropna(subset=['AverageTemperature','AverageTemperatureUncertainty'])
    
    # drop duplicate rows
    df_temperature_clean = df_temperature_clean.drop_duplicates(subset=['dt', 'City', 'Country'])
    
    return df_temperature_clean


def clean_demographics_data(input_data):
    """Clean the US demographics dataset
    
    Args:
        input_data: spark dataframe immigration dataframe
    
    Returns:
        df_demographics: spark dataframe arrive date dimension table
    """
    
    # check missing values
    df_missing = utils.check_missing_values(input_data)
    
    # get columns to drop which have missing values over 50%
    drop_cols = utils.get_cols_to_drop(df_missing, 0.5)
    
    if drop_cols:
        # drop the columns which have missing data > 50% since it won't be helpful for analysis
        df_demographics_dropped = input_data.drop(*drop_cols)

    # drop rows with missing values
    df_demographics_dropped = input_data.dropna()
    
    # drop duplicate columns
    df_demographics_clean = df_demographics_dropped.dropDuplicates(subset=['City', 'State', 'State Code', 'Race'])
    
    return df_demographics_clean