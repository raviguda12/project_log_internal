# file locations

isi_py_file = r"modules/increase_shared_input.py"
kss_py_file = r"modules/kafka_s3_sink.py"
rl_py_file = r"modules/raw_layer.py"
cll_py_file = r"modules/cleansed_layer.py"
cul_py_file = r"modules/curated_layer.py"

# AWS

aws_access_key_id = r"**"
aws_secret_access_key = r"**"
bootstrap_servers = r"b-1.managedkafkademoclust.za5j2l.c19.kafka.us-east-1.amazonaws.com:9092,b-2.managedkafkademoclust.za5j2l.c19.kafka.us-east-1.amazonaws.com:9092,b-3.managedkafkademoclust.za5j2l.c19.kafka.us-east-1.amazonaws.com:9092"
kafka_topic = r"kafka-processed-latest"


# HIVE

hive_db = r"log"
hive_raw_table = r"raw_log_details"
hive_cleansed_table = r"cleansed_log_details"
hive_curated_table = r"curated_log_details"
hive_log_agg_per_device_table = r"log_agg_per_device"
hive_log_agg_across_device_table = r"log_agg_across_device"

# Development

shared_input_path = r"internal_files/shared_input/shared_input.txt"
processed_input_path = r"internal_files/project-demo-processed-input/project-demo-processed-input.txt"
processed_input_dummy_path = r"internal_files/project-demo-processed-input/project-demo-dummy-processed-input.txt"
kafka_processed_input_path = r"internal_files/kafka-processed-input/kafka-processed-input.txt"
processed_input_count = 1000
rows_to_take = 300
raw_layer_df_path = r"internal_files/raw_log_details.csv"
cleansed_layer_df_path = r"internal_files/cleansed_log_details.csv"
curated_layer_df_path = r"internal_files/curated_log_details.csv"
log_agg_per_device_df_path = r"internal_files/log_agg_per_device.csv"
log_agg_across_device_df_path = r"internal_files/log_agg_across_device.csv"
