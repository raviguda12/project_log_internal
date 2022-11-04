# project_log_internal

## Project Description

* **Input source data :** Log from different ip machines like phone and browsers for the different requests raised
* **Data Source Identification :** Need to mock the data present in the above sample log data with different incremental timestamp to load the data via kafka into the various data lake layers like raw/cleansed/Curated.
* **Data storage:** The data identified from the data store need to be collected into the staging layers for duplication, clean-up and data transformation as suggested in the data model for the three layers. Only the final consumption layer also needs to be available in Snowflake too.
* **Data aggregation and reporting:** Cleansed and transformed data need to be stored in a granular level log details table which has info on the client_id with method and status code level details.

**Note :** Also aggregate level per device and across devices need to be stored for analytical purposes.
# HIVE

`hive_db` raw_log_details,cleansed_log_details`curated_log_details` `log_agg_per_device_table` `log_agg_across_device_table`

#### SNOWFLAKE

`sfURL` `sfAccount` `sfUser` `sfPassword` `sfDatabase` `sfSchema` `sfWarehouse` `sfRole` raw_log_details,cleansed_log_details`curated_log_details `log_agg_per_device_table` `log_agg_across_device_table`


## Tech Stack

**Client** 

`Python`

`PySpark`

**Server**

`Managed Kafka`

`AWS`

`HIVE`

`SNOWFLAKE `

`S3`


