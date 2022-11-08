import os
import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
sys.path.append(str(Path.cwd().parent.parent))
from pyspark.sql import SparkSession
from helpers.snowflake_helper import SnowflakeHelper
from helpers.spark_helper import SparkHelper

spark = SparkHelper().get_spark_session()

log_per_device_df = spark.sql("select * from log.log_agg_per_device limit 10;")
cleansed_data_df =     spark.sql("""select * from log.cleansed_log_details where `client_ip` in (select `client_ip` from log.log_agg_per_device limit 10)""")
print("log agg per device : first 10 ip's --> \n")
log_per_device_df.show()
print("\nCleansed Data check --> \n")
cleansed_data_df.show()

SnowflakeHelper().save_df_to_snowflake(cleansed_data_df, "test_transaction_details")
