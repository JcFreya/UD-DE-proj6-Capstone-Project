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


def create_arrdate_dimension(input_data, output_data):
    """Create the arrive date dimension table using arrdate in immigration data
    
    Args:
        input_data: dataframe immigration dataframe
    
    Returns:
        output_data: spark dataframe arrive date dimension table
    """
    
    # format the sas date in arrdate column
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    # create date dimension table using arrdate
    df_arrdate_dim = input_data.select(['arrdate']).withColumn("arrdate", get_date(input_data.arrdate)).distinct()
    
    # add other dimension of date
    df_arrdate_dim = df_arrdate_dim.withColumn('arrival_day', dayofmonth('arrdate'))
    df_arrdate_dim = df_arrdate_dim.withColumn('arrival_week', weekofyear('arrdate'))
    df_arrdate_dim = df_arrdate_dim.withColumn('arrival_month', month('arrdate'))
    df_arrdate_dim = df_arrdate_dim.withColumn('arrival_year', year('arrdate'))
    df_arrdate_dim = df_arrdate_dim.withColumn('arrival_weekday', dayofweek('arrdate'))

    # add an identical column
    df_arrdate_dim = df_arrdate_dim.withColumn('id', monotonically_increasing_id())
    
    # write to parquet file
    partition_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    df_arrdate_dim.write.parquet(output_data + "immigration_arrdate", partitionBy=partition_columns, mode="overwrite")
    
    return df_arrdate_dim


def create_country_dimension(spark, input_data, df_temperature, output_data, country_cd):
    
    """Extract the country dimension using the immigration and land temperature dataest
    
    Args:
        spark: spark session object
        input_data: spark dataframe of immigration events
        temp_df: spark dataframe of global land temperatures data.
        output_data: path to write dimension dataframe
        country_cd: csv file which has country codes and country names mapping
    
    Returns:
        df_country: spark dataframe output dimension dataframe
    """
    
    # create temporary view for immigration data
    input_data.createOrReplaceTempView("immigration_view")

    # create temporary view for countries codes data
    country_cd.createOrReplaceTempView("country_codes_view")

    # aggregate the temperature data
    agg_temp = df_temperature.select(['Country', 'AverageTemperature']).groupby('Country').avg()
    agg_temp = agg_temp.withColumnRenamed('avg(AverageTemperature)', 'average_temperature')
    
    # create temporary view for average temperature data
    agg_temp.createOrReplaceTempView("average_temperature_view")

    # etract country dimension
    df_country_dim = spark.sql(
        """
        SELECT 
            i94res as country_code,
            Name as country_name
        FROM immigration_view
        LEFT JOIN country_codes_view
        ON immigration_view.i94res=country_codes_view.Code
        """
    ).distinct()
    
    # create country view
    df_country_dim.createOrReplaceTempView("country_view")

    df_country_dim = spark.sql(
        """
        SELECT 
            country_code,
            country_name,
            average_temperature
        FROM country_view
        LEFT JOIN average_temperature_view
        ON country_view.country_name=average_temperature_view.Country
        """
    ).distinct()

    # write the dimension to a parquet file
    df_country_dim.write.parquet(output_data + "country", mode="overwrite")

    return df_country_dim


def create_visa_dimension(input_data, output_data):
    """Create the visa dimension table using immigration data
    
    Args:
        input_data: spark dataframe of immigration events
        output_data: path to write dimension dataframe
    
    Returns:
        df_visa: spark dataframe of visa dimension table
    """
    
    # create visa dimension dataframe using visatype column
    df_visa = input_data.select(['visatype']).distinct()

    # add an nonduplicate id column
    df_visa_dim = df_visa.withColumn('visa_type_key', monotonically_increasing_id())

    # write dimension to parquet file
    df_visa_dim.write.parquet(output_data + "visa", mode="overwrite")

    return df_visa_dim


def create_demographics_dimension(input_data, output_data):
    """Create demographic dimension tables using US demographics dataset
    
    Args:
        input_data: spark dataframe of us demographics survey data
        output_data: path to write dimension dataframe to
    
    Returns:
        df_demographics: spark dataframe of demographics dimension
    """
    df_demographics_dim = input_data.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'average_household_size') \
        .withColumnRenamed('State Code', 'state_code')
    # add an un duplicate id column
    df_demographics_dim = df_demographics_dim.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file
    df_demographics_dim.write.parquet(output_data + "demographics", mode="overwrite")

    return df_demographics_dim


def create_immigration_fact(spark, input_data, output_data):
    """Create the immigration fact table with immigration and temperature dataset
    
    Args:
        spark: spark session
        input_data: spark dataframe of immigration events
        output_data: path to write dimension dataframe to
    
    Returns:
        df_fact: spark dataframe representing calendar dimension
    """
    # load visa dimension
    df_visa = spark.read.parquet(output_data + "visa")

    # create a view for visa type dimension
    df_visa.createOrReplaceTempView("visa_view")

    # convert arrival date in SAS format to datetime
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # rename columns to align with data model
    df_fact = input_data.withColumnRenamed('ccid', 'record_id') \
        .withColumnRenamed('i94res', 'country_residence_code') \
        .withColumnRenamed('i94addr', 'state_code')

    # create an immigration view
    df_fact.createOrReplaceTempView("immigration_view")

    # create visa_type key
    df_fact = spark.sql(
        """
        SELECT 
            immigration_view.*, 
            visa_view.visa_type_key
        FROM immigration_view
        LEFT JOIN visa_view ON visa_view.visatype=immigration_view.visatype
        """
    )

    # convert arrival date into datetime object
    df_fact = df_fact.withColumn("arrdate", get_date(df_fact.arrdate))

    # drop visatype key
    df_fact = df_fact.drop(df_fact.visatype)

    # write dimension to parquet file
    df_fact.write.parquet(output_data + "immigration_fact", mode="overwrite")

    return df_fact