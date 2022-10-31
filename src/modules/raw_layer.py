import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

import env


def create_raw_layer():
    """
    The create_raw_layer function reads the raw data from a Kafka topic and writes it to a Hive table.
    The function also creates an external table in HDFS that contains the raw data as CSV files.


    :return: A dataframe with the following columns:
    """
    spark = SparkSession.builder.appName("Demo-Project2").config("spark.master",
                                                                 "local").enableHiveSupport().getOrCreate()
    df = spark.read.option("delimiter", " ").csv("{}".format(env.kafka_file_path))
    df_col = (df.select(
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
    df_col.write.mode("overwrite").format('csv').option("header", True).save(
        "{}".format(env.raw_df_path))
    df_col.write.mode("overwrite").saveAsTable("{}".format(env.raw_hive_table))


if __name__ == "__main__":
    create_raw_layer()
