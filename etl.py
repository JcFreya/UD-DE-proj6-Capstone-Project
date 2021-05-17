# All imports and installs
import pandas as pd
import os
import configparser

import datetime as dt

import utils

# Spark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def process_immigration_data(spark, input_data, output_data, file_name, temperature_file, mapping_file):
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
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)

    # clean immigration spark dataframe
    immigration_df = utils.clean_immigration_data(immigration_df)

    # create visa_type dimension table
    visatype_df = utils.create_visa_dimension(immigration_df, output_data)

    # create calendar dimension table
    arrdate_df = utils.create_arrdate_dimension(immigration_df, output_data)

    # get global temperatures data    
    file = input_data + temperature_file
    df_temperature = spark.read.csv(file, header=True, inferSchema=True)

    # clean the temperature data
    temp_df = utils.clean_temperature_data(df_temperature)

    # create country dimension table
    dim_df = utils.create_country_dimension(spark, immigration_df, temp_df, output_data, mapping_file)

    # create immigration fact table
    fact_df = utils.create_immigration_fact(spark, immigration_df, output_data)


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
    demographics_df = spark.read.csv(file, inferSchema=True, header=True, sep=';')

    # clean demographics data
    clean_demographics_df = utils.clean_demographics_data(demographics_df)

    # create demographic dimension table
    df_demographics_dim = utils.create_demographics_dimension(clean_demographics_df, output_data)


def main():
    # create spark session
    spark = utils.create_spark_session()
    input_data = "s3://sparkprojectdata/"
    output_data = "s3://sparkprojectdata/"

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