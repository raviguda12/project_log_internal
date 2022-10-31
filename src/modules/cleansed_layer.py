import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace

import env


def create_cleansed_layer():
    """
    The create_cleansed_layer function reads the raw data from a CSV file, and creates a cleansed layer of the data.
    The cleansed layer is then written to an output directory as a set of Parquet files. The function also writes
    the cleansed layer to Hive as an external table.

    :return: A cleansed dataframe and a hive table
    """
    spark = SparkSession.builder.config("spark.master", "local").enableHiveSupport().getOrCreate()
    df = spark.read.option("delimiter", " ").csv("{}".format(env.raw_df_path))
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
    remove_spec = cleansed_data.select('request', regexp_replace('request', '%|,|-|\?=', ''))
    cleansed_data = cleansed_data.withColumn('request', regexp_replace('request', '%|,|-|\?=', ''))
    cleansed_data.na.fill("Nan").show(truncate=False)
    cleansed_data1 = cleansed_data.withColumn("size", round(col("size") / 1024, 2))
    final_cleansed = cleansed_data1.withColumn('method', regexp_replace('method', 'GET', 'PUT'))
    final_cleansed.write.mode("overwrite").format('csv').option("header", True).save("{}".format(env.cleansed_df_path))
    final_cleansed.write.mode("overwrite").saveAsTable("{}".format(env.cleansed_hive_table))


if __name__ == "__main__":
    create_cleansed_layer()
