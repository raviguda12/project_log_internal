import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace

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
    spark = SparkSession.builder.appName("Demo-Project2").config("spark.master",
                                                                 "local").enableHiveSupport().getOrCreate()
    df = spark.read.option("delimiter", " ").csv("{}".format(env.cleansed_df_path))
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
    df_clean = df_col.withColumn('datetime', regexp_replace('datetime', '\[|\]|', ''))
    df_date = df_clean.withColumn("datetime", to_timestamp("datetime", "dd/MMM/yyyy:HH:mm:ss")).withColumn('datetime',
                                                                                                           date_format(
                                                                                                               col("datetime"),
                                                                                                               "MM/dd/yyyy HH:mm:ss"))
    cleaned_df = df_date.withColumn("referer_present(YorN)",
                                    when(col("referrer") == "-", "N") \
                                    .otherwise("Y"))
    cleansed_data = cleaned_df.drop("referrer")
    cleansed_data = cleansed_data.withColumn('request', regexp_replace('request', '%|,|-|\?=', ''))
    cleansed_data.na.fill("Nan").show(truncate=False)
    cleansed_data1 = cleansed_data.withColumn("size", round(col("size") / 1024, 2))
    final_cleansed = cleansed_data1.withColumn('method', regexp_replace('method', 'GET', 'PUT'))
    final_cleansed.show(truncate=False)
    final_cleansed.write.mode("overwrite").saveAsTable("curated_data_table")
    split_date_udf = udf(lambda x: split_date(x), StringType())
    cnt_cond = lambda cond: sum(when(cond, 1).otherwise(0))
    log_agg_per_device = final_cleansed.withColumn("day_hour", split_date_udf(col("datetime"))).groupBy("day_hour",
                                                                                                        "client_ip") \
        .agg(cnt_cond(col('method') == "GET").alias("no_get"),
             cnt_cond(col('method') == "POST").alias("no_post"),
             cnt_cond(col('method') == "HEAD").alias("no_head"),
             ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
        .select("row_id", "day_hour", "client_ip", "no_get", "no_post", "no_head")
    log_agg_per_device.write.mode("overwrite").format('csv').option("header", True).save("{}".format(env.per_device_df_path))
    log_agg_per_device.write.mode("overwrite").saveAsTable("{}".format(env.per_device_hive_table))
    log_agg_across_device = log_agg_per_device.groupBy("day_hour") \
        .agg(count(col("client_ip")).alias("no_of_clients"),
             sum(col('no_get')).alias("no_get"),
             sum(col('no_post')).alias("no_post"),
             sum(col('no_head')).alias("no_head"),
             ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
        .select("row_id", "day_hour", "no_of_clients", "no_put", "no_post", "no_head")
    log_agg_across_device.write.mode("overwrite").format('csv').option("header", True).save("{}".format(env.across_device_df_path))
    log_agg_across_device.write.mode("overwrite").saveAsTable("{}".format(env.across_device_hive_table))


if __name__ == "__main__":
    create_curated_layer()
