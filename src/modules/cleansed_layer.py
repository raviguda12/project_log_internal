import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace

import env


def create_cleansed_layer(raw_path, cleansed_path, hive_db, hive_table):
    """
    The create_cleansed_layer function reads the raw data from a CSV file, and creates a cleansed layer of the data.
    The cleansed layer is then written to an output directory as a set of Parquet files. The function also writes
    the cleansed layer to Hive as an external table.

    :return: A cleansed dataframe and a hive table
    """
    spark = SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                                                            'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
    df = spark.read.option("delimiter", ",").option("header", True).csv(raw_path)
    print(df.columns)
    df = df.withColumn('datetime', regexp_replace(col('datetime'), r'\[|\]|', ''))
    df = df.withColumn("datetime", to_timestamp(col("datetime"), "dd/MMM/yyyy:HH:mm:ss")).withColumn('datetime',
                                                                                                date_format(
                                                                                                    col("datetime"),
                                                                                                    "MM/dd/yyyy HH:mm:ss"))
    df = df.withColumn("referer_present(YorN)",
                       when(col("referrer") == "-", "N") \
                       .otherwise("Y"))
    df = df.drop("referrer")
    remove_spec = df.select('request', regexp_replace('request', '%|,|-|\?=', ''))
    df = df.withColumn('request', regexp_replace('request', '%|,|-|\?=', ''))
    df.na.fill("Nan").show(truncate=False)
    df = df.withColumn("size", round(col("size") / 1024, 2))
    # df = df.withColumn('method', regexp_replace('method', 'GET', 'POST'))
    df.write.mode("overwrite").format('csv').option("header", True).save(cleansed_path)
    df.write.mode("overwrite").saveAsTable("{}.{}".format(hive_db, hive_table))
    sfOptions = {
        "sfURL": "https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
        "sfAccount": "tm57257",
        "sfUser": "TESTDATA",
        "sfPassword": "Welcome@1",
        "sfDatabase": "LOGDEMO",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"
    }

    df.write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format("cleansed_log_details")).mode(
        "append").options(header=True).save()


if __name__ == "__main__":
    create_cleansed_layer(r"{}/{}".format(os.getcwd(), env.raw_layer_df_path),
                          r"{}/{}".format(os.getcwd(), env.cleansed_layer_df_path),
                          env.hive_db,
                          env.hive_cleansed_table)
