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
import clean_data
import create_tables
import quality_checks

# Spark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear, upper
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def process_immigration_data(spark, input_data, output_data, file_name, temperature_file, country_cd):
    """Load in immigration data and then create fact table, arrdate, visa and country dimension tables
    
    Args:
        spark: SparkSession spark session instance
        input_data: string input file path
        output_data: string output file path
        file_name: string immigration input file name
        country_code: dataframe maps of country codes to country names
        temperature_file: string global temperatures input file name
    """
    # get the file path to the immigration data
    immigration_file = input_data + file_name

    # read immigration data file
    immigration_df_raw = spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)

    # clean immigration spark dataframe
    immigration_df_clean = clean_data.clean_immigration_data(immigration_df_raw)

    # create visa_type dimension table
    visatype_df = create_tables.create_visa_dimension(immigration_df_clean, output_data)

    # create calendar dimension table
    arrdate_df = create_tables.create_arrdate_dimension(immigration_df_clean, output_data)

    # get global temperatures data    
    file = input_data + temperature_file
    df_temperature_raw = spark.read.csv(file, header=True, inferSchema=True)

    # clean the temperature data
    temperature_df_clean = clean_data.clean_temperature_data(df_temperature_raw)

    # create country dimension table
    dim_df = create_tables.create_country_dimension(spark, immigration_df_clean, temperature_df_clean, output_data, country_cd)

    # create immigration fact table
    fact_df = create_tables.create_immigration_fact(spark, immigration_df_clean, output_data)


def process_demographics_data(spark, input_data, output_data, file_name):
    """Process the demographics data and create the demographics dimension table
    
    Args:
        spark: SparkSession spark session instance
        input_data: string input file path
        output_data: string output file path
        file_name: string immigration input file name
    """

    # load demographics data
    file = input_data + file_name
    demographics_df_raw = spark.read.csv(file, inferSchema=True, header=True, sep=';')

    # clean demographics data
    demographics_df_clean = clean_data.clean_demographics_data(demographics_df_raw)

    # create demographic dimension table
    df_demographics_dim = create_tables.create_demographics_dimension(demographics_df_clean, output_data)


def main():
    # create spark session
    spark = utils.create_spark_session()
    
    # set up data location
    input_data = "s3://sparkprojectdata/"
    output_data = "s3://sparkprojectdata/"
    
    # set up data file name
    immigration_file_name = 'i94_apr16_sub.sas7bdat'
    temperature_file_name = 'GlobalLandTemperaturesByCity.csv'
    demographics_file_name = 'us-cities-demographics.csv'
    
    # load the i94res to country mapping data
    country_cd_file_name = input_data + "i94res.csv"
    country_cd = spark.read.csv(country_cd_file_name, header=True, inferSchema=True)

    process_immigration_data(spark, input_data, output_data, immigration_file_name, temperature_file_name, country_cd)

    process_demographics_data(spark, input_data, output_data, demographics_file_name)
    

if __name__ == "__main__":
    main()