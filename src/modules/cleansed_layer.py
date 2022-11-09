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
from pyspark.sql.functions import regexp_replace
from helpers.snowflake_helper import SnowflakeHelper

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
    df = df.drop(col("row_id")).dropDuplicates().withColumn("row_id", monotonically_increasing_id())
    df = df.select('row_id', 'client_ip', 'datetime', 'method', 'request', 'status_code', 'size', 'referrer', 'user_agent')
    df = df.withColumn("datetime", to_timestamp(col("datetime"), "dd/MMM/yyyy:HH:mm:ss")).withColumn('datetime',
                                                                                                date_format(
                                                                                                    col("datetime"),
                                                                                                    "MM/dd/yyyy HH:mm:ss"))
    df = df.withColumn("referer_present(YorN)",
                       when(col("referrer") == "NA", "N") \
                       .otherwise("Y"))
    df = df.drop("referrer")
    df.na.fill("Nan").show(truncate=False)

    def convert_to_kb(val):
        return str(int(val) / (10 ** 3)) + " KB"
    convert_to_kb_udf = F.udf(lambda x: convert_to_kb(x), StringType())
    df = df.withColumn("size", convert_to_kb_udf(col("size")))
    # df = df.withColumn('method', regexp_replace('method', 'GET', 'POST'))
    df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(cleansed_path)
    df.coalesce(1).write.mode("overwrite").saveAsTable("{}.{}".format(hive_db, hive_table))
    return df

if __name__ == "__main__":
    df = create_cleansed_layer(r"{}/{}".format(os.getcwd(), env.raw_layer_df_path),
                          r"{}/{}".format(os.getcwd(), env.cleansed_layer_df_path),
                          env.hive_db,
                          env.hive_cleansed_table)
    SnowflakeHelper().save_df_to_snowflake(df, env.sf_cleansed_table)
