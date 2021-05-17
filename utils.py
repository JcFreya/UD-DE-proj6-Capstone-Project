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


def creat_spark_session():
    """
    Create and return spark session

    Args:
        None

    Returns:
        spark: Spark Session

    """
    spark = SparkSession \
        .builder() \
#         .config("spark.jars.packages")\
#         .enableHiveSupport()\
        .getOrCreate()
    return spark

def clean_immigration_data(input_data):
    """Clean immigration dataframe
    
    Args:
        input_data: spark dataframe with monthly immigration data
    
    Returns:
        df_immigration: clean dataframe
    """
    
    df_demographics_pd = input_data.toPandas()
    
    df_immigration_selected = df_demographics_pd[['cicid', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate',
                                          'i94mode', 'i94addr', 'depdate', 'i94bir', 'visatype',
                                          'count', 'visapost', 'occup', 'gender', 'airline']]
    # check missing values
    df_immigration_missing = df_immigration_selected.isnull().sum()/df_immigration_selected.shape[0]
    
    # get columns to drop which have missing values over 50%
    drop_cols = list(df_immigration_missing[df_immigration_missing>0.5].index.values)

    df_immigration = input_data.drop(*drop_cols)
    
    # drop rows where all elements are missing
    df_immigration = df_immigration.dropna(how='all')

    return df_immigration


def clean_temperature_data(input_data):
    """Clean temperatures dataset
    
    Args:
        input_data: dataframe immigration dataframe
    
    Returns:
        df_temperature: spark dataframe arrive date dimension table
    """
    
    # drop rows with missing average temperature
    input_data = input_data.dropna(subset=['AverageTemperature','AverageTemperatureUncertainty'])
    
    # drop duplicate rows
    df_temperature = input_data.drop_duplicates(subset=['dt', 'City', 'Country'])
    
    return df_temperature


def clean_demographics_data(input_data):
    """Clean the US demographics dataset
    
    Args:
        input_data: spark dataframe immigration dataframe
    
    Returns:
        df_demographics: spark dataframe arrive date dimension table
    """
    # drop rows with missing values
    df_demographics = input_data.dropna()
    
    # drop duplicate columns
    df_demographics = df_demographics.dropDuplicates(subset=['City', 'State', 'State Code', 'Race'])
    
    return df_demographics


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
    df_arrdate = df.select(['arrdate']).withColumn("arrdate", get_date(df.arrdate)).distinct()
    
    # add other dimension of date
    df_arrdate = df_arrdate.withColumn('arrival_day', dayofmonth('arrdate'))
    df_arrdate = df_arrdate.withColumn('arrival_week', weekofyear('arrdate'))
    df_arrdate = df_arrdate.withColumn('arrival_month', month('arrdate'))
    df_arrdate = df_arrdate.withColumn('arrival_year', year('arrdate'))
    df_arrdate = df_arrdate.withColumn('arrival_weekday', dayofweek('arrdate'))

    # add an identical column
    df_arrdate = df_arrdate.withColumn('id', monotonically_increasing_id())
    
    # write to parquet file
    partition_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    df_arrdate.write.parquet(output_data + "immigration_arrdate", partitionBy=partition_columns, mode="overwrite")
    
    return df_arrdate


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
    agg_temp = new_df.withColumnRenamed('avg(AverageTemperature)', 'average_temperature')
    
    # create temporary view for average temperature data
    agg_temp.createOrReplaceTempView("average_temperature_view")

    # etract country dimension
    df_country = spark.sql(
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
    df_country.createOrReplaceTempView("country_view")

    df_country = spark.sql(
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
    df_country.write.parquet(output_data + "country", mode="overwrite")

    return df_country


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
    df_visa = df_visa.withColumn('visa_type_key', monotonically_increasing_id())

    # write dimension to parquet file
    df_visa.write.parquet(output_data + "visatype", mode="overwrite")

    return df_visa


def create_demographics_dimension(input_data, output_data):
    """Create demographic dimension tables using US demographics dataset
    
    Args:
        input_data: spark dataframe of us demographics survey data
        output_data: path to write dimension dataframe to
    
    Returns:
        df_demographics: spark dataframe of demographics dimension
    """
    df_demographics = input_data.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'average_household_size') \
        .withColumnRenamed('State Code', 'state_code')
    # add an un duplicate id column
    df_demographics = df_demographics.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file
    df_demographics.write.parquet(output_data + "demographics", mode="overwrite")

    return df_demographics


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
    df_visa = spark.read.parquet(output_data + "visatype")

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


def quality_checks(input_data, table_name):
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


def count_checks(source_table, table):
    """Check the count of rows of fact and dimension tables the same as source table
    
    Args:
        source_table: spark dataframe to check counts on
        table_name: corresponding name of table
    """
    source_table_count = source_table.count()
    table_count = table.count()

    if source_table_count == table_count:
        print(f"Data quality check failed for loaded table has data missing while trasfering from source data")
    else:
        print(f"Data quality check passed for loaded table has same number of rows as source data")
   
