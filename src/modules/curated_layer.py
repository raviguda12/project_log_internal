import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
sys.path.append(str(Path.cwd().parent.parent))
import os

import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
from helpers.snowflake_helper import SnowflakeHelper

import env


def split_date(val):
    """
    The split_date function splits the date from the time and returns only the date.

    :param val: Pass the value of the date column to the split_date function
    :return: A list of the values split on a colon
    """
    return val.split(":")[0]


def create_curated_layer():
    """
    The create_curated_layer function creates a curated layer of the data.
    It takes in the raw data and cleans it by removing unwanted characters,
    converting to appropriate datatypes, and splitting up columns into multiple columns.
    The cleaned data is then saved as a Hive table for further processing.

    :return: The following:
    """
    spark = SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                                                            'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
    df = spark.read.option("delimiter", ",").option("header", True).csv("{}/{}".format(os.getcwd(), env.cleansed_layer_df_path))
    print(df.columns)
    df = df.withColumn("status_code", col("status_code").cast("int")).withColumn("row_id", col("row_id").cast("int"))
    df.write.mode("overwrite").format('csv').option("header", True).save("{}/{}".format(os.getcwd(), env.curated_layer_df_path))
    df.write.mode("overwrite").saveAsTable("log.curated_log_details")
    SnowflakeHelper().save_df_to_snowflake(df, env.sf_curated_table)
    split_date_udf = udf(lambda x: split_date(x), StringType())
    cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))
    log_agg_per_device = df.withColumn("day_hour", split_date_udf(col("datetime"))).groupBy("day_hour",
                                                                                                        "client_ip") \
        .agg(cnt_cond(col('method') == "GET").alias("no_get"),
             cnt_cond(col('method') == "POST").alias("no_post"),
             cnt_cond(col('method') == "HEAD").alias("no_head"),
             ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
        .select("row_id", "day_hour", "client_ip", "no_get", "no_post", "no_head")
    log_agg_per_device.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save("{}/{}".format(os.getcwd(), env.log_agg_per_device_df_path))
    log_agg_per_device.coalesce(1).write.mode("overwrite").saveAsTable("log.{}".format(env.hive_log_agg_per_device_table))
    SnowflakeHelper().save_df_to_snowflake(log_agg_per_device, env.sf_log_agg_per_device_table)
    log_agg_across_device = log_agg_per_device.groupBy("day_hour") \
        .agg(count(col("client_ip")).alias("no_of_clients"),
             sum(col('no_get')).alias("no_get"),
             sum(col('no_post')).alias("no_post"),
             sum(col('no_head')).alias("no_head"),
             ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
        .select("row_id", "day_hour", "no_of_clients", "no_get", "no_post", "no_head")
    log_agg_across_device.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save("{}/{}".format(os.getcwd(), env.log_agg_across_device_df_path))
    log_agg_across_device.coalesce(1).write.mode("overwrite").saveAsTable("log.{}".format(env.hive_log_agg_across_device_table))
    SnowflakeHelper().save_df_to_snowflake(log_agg_across_device, env.sf_log_agg_across_device_table)

if __name__ == "__main__":
    create_curated_layer()
