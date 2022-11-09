import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
sys.path.append(str(Path.cwd().parent.parent))
import os

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from helpers.snowflake_helper import SnowflakeHelper

import env


def create_raw_layer(processed_path, raw_path, hive_db, hive_table):
    """
    The create_raw_layer function reads the raw data from a Kafka topic and writes it to a Hive table.
    The function also creates an external table in HDFS that contains the raw data as CSV files.


    :return: A dataframe with the following columns:
    """
    spark = SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                                                     'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
    df = spark.read.option("delimiter", " ").csv(processed_path)
    df = (df.select(
        F.monotonically_increasing_id().alias('row_id'),
        F.col("_c0").alias("client_ip"),
        F.split(F.col("_c3"), " ").getItem(0).alias("datetime"),
        F.split(F.col("_c5"), " ").getItem(0).alias("method"),
        F.split(F.col("_c5"), " ").getItem(1).alias("request"),
        F.col("_c6").alias("status_code"),
        F.col("_c7").alias("size"),
        F.col("_c8").alias("referrer"),
        F.col("_c9").alias("user_agent")
    ))
    # df.show()
    def convert_to_na(val):
        if val=="-":
            return "NA"
        else:
            return str(val)
    convert_to_na_udf = F.udf(lambda x: convert_to_na(x), StringType())
    df = df.withColumn("referrer", convert_to_na_udf(col("referrer")))
    df = df.withColumn('datetime', regexp_replace(col('datetime'), r'\[|\]|', ''))
    df = df.withColumn('request', regexp_replace('request', '%|,|-|\?=', ''))
    df = df.withColumn('referrer', regexp_replace('referrer', '%|,|-|\?=', ''))
    df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(raw_path)
    spark.sql("create database IF NOT EXISTS {}".format(hive_db))
    df.coalesce(1).write.mode("overwrite").saveAsTable("{}.{}".format(hive_db, hive_table))
    return df


if __name__ == "__main__":
    df = create_raw_layer(r"{}/{}".format(os.getcwd(), env.processed_input_path),
                     r"{}/{}".format(os.getcwd(), env.raw_layer_df_path),
                     env.hive_db,
                     env.hive_raw_table)
    SnowflakeHelper().save_df_to_snowflake(df, env.sf_raw_table)
