// Databricks notebook source
// MAGIC %sql
// MAGIC create database if not exists nyc_db;

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_yellow_raw_prq;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_yellow_raw_prq(
// MAGIC taxi_type STRING,
// MAGIC vendor_id STRING,
// MAGIC pickup_datetime TIMESTAMP,
// MAGIC dropoff_datetime TIMESTAMP,
// MAGIC store_and_fwd_flag STRING,
// MAGIC rate_code_id INT,
// MAGIC pickup_location_id INT,
// MAGIC dropoff_location_id INT,
// MAGIC pickup_longitude STRING,
// MAGIC pickup_latitude STRING,
// MAGIC dropoff_longitude STRING,
// MAGIC dropoff_latitude STRING,
// MAGIC passenger_count INT,
// MAGIC trip_distance DOUBLE,
// MAGIC fare_amount DOUBLE,
// MAGIC extra DOUBLE,
// MAGIC mta_tax DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC tolls_amount DOUBLE,
// MAGIC improvement_surcharge DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC payment_type STRING,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING)
// MAGIC USING parquet
// MAGIC partitioned by (trip_year,trip_month)
// MAGIC LOCATION 'wasbs://raw@gaiasa.blob.core.windows.net/yellow-taxi/';
// MAGIC 
// MAGIC --

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_green_raw_prq;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_green_raw_prq(
// MAGIC taxi_type STRING,
// MAGIC vendor_id INT,
// MAGIC pickup_datetime TIMESTAMP,
// MAGIC dropoff_datetime TIMESTAMP,
// MAGIC store_and_fwd_flag STRING,
// MAGIC rate_code_id INT,
// MAGIC pickup_location_id INT,
// MAGIC dropoff_location_id INT,
// MAGIC pickup_longitude STRING,
// MAGIC pickup_latitude STRING,
// MAGIC dropoff_longitude STRING,
// MAGIC dropoff_latitude STRING,
// MAGIC passenger_count INT,
// MAGIC trip_distance DOUBLE,
// MAGIC fare_amount DOUBLE,
// MAGIC extra DOUBLE,
// MAGIC mta_tax DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC tolls_amount DOUBLE,
// MAGIC ehail_fee DOUBLE,
// MAGIC improvement_surcharge DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC payment_type INT,
// MAGIC trip_type INT,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING)
// MAGIC USING parquet
// MAGIC partitioned by (trip_year,trip_month)
// MAGIC LOCATION 'wasbs://raw@gaiasa.blob.core.windows.net/green-taxi/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS refdata_taxi_zone_lookup;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS refdata_taxi_zone_lookup(
// MAGIC location_id STRING,
// MAGIC borough STRING,
// MAGIC zone STRING,
// MAGIC service_zone STRING)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://refdata@gaiasa.blob.core.windows.net/taxi-zone/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS refdata_trip_month_lookup;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS refdata_trip_month_lookup(
// MAGIC trip_month STRING,
// MAGIC month_name_short STRING,
// MAGIC month_name_full STRING)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://refdata@gaiasa.blob.core.windows.net/trip-month/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS refdata_rate_code_lookup;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS refdata_rate_code_lookup(
// MAGIC rate_code_id INT,
// MAGIC description STRING)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://refdata@gaiasa.blob.core.windows.net/rate-code/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS refdata_payment_type_lookup;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS refdata_payment_type_lookup(
// MAGIC payment_type INT,
// MAGIC abbreviation STRING,
// MAGIC description STRING)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://refdata@gaiasa.blob.core.windows.net/payment-type/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS refdata_trip_type_lookup;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS refdata_trip_type_lookup(
// MAGIC trip_type INT,
// MAGIC description STRING)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://refdata@gaiasa.blob.core.windows.net/trip-type/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS refdata_vendor_lookup;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS refdata_vendor_lookup(
// MAGIC vendor_id INT,
// MAGIC abbreviation STRING,
// MAGIC description STRING)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://refdata@gaiasa.blob.core.windows.net/vendor/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_yellow_transformed_prq;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_yellow_transformed_prq(
// MAGIC taxi_type STRING,
// MAGIC vendor_id STRING,
// MAGIC pickup_datetime TIMESTAMP,
// MAGIC dropoff_datetime TIMESTAMP,
// MAGIC store_and_fwd_flag STRING,
// MAGIC rate_code_id INT,
// MAGIC pickup_location_id INT,
// MAGIC dropoff_location_id INT,
// MAGIC pickup_longitude STRING,
// MAGIC pickup_latitude STRING,
// MAGIC dropoff_longitude STRING,
// MAGIC dropoff_latitude STRING,
// MAGIC passenger_count INT,
// MAGIC trip_distance DOUBLE,
// MAGIC fare_amount DOUBLE,
// MAGIC extra DOUBLE,
// MAGIC mta_tax DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC tolls_amount DOUBLE,
// MAGIC improvement_surcharge DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC payment_type STRING,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING,
// MAGIC vendor_abbreviation STRING,
// MAGIC vendor_description STRING,
// MAGIC month_name_short STRING,
// MAGIC month_name_full STRING,
// MAGIC payment_type_description STRING,
// MAGIC rate_code_description STRING,
// MAGIC pickup_borough STRING,
// MAGIC pickup_zone STRING,
// MAGIC pickup_service_zone STRING,
// MAGIC dropoff_borough STRING,
// MAGIC dropoff_zone STRING,
// MAGIC dropoff_service_zone STRING,
// MAGIC pickup_year INT,
// MAGIC pickup_month INT,
// MAGIC pickup_day INT,
// MAGIC pickup_hour INT,
// MAGIC pickup_minute INT,
// MAGIC pickup_second INT,
// MAGIC dropoff_year INT,
// MAGIC dropoff_month INT,
// MAGIC dropoff_day INT,
// MAGIC dropoff_hour INT,
// MAGIC dropoff_minute INT,
// MAGIC dropoff_second INT)
// MAGIC USING parquet
// MAGIC partitioned by (trip_year,trip_month)
// MAGIC LOCATION 'wasbs://transformed@gaiasa.blob.core.windows.net/yellow-taxi/';
// MAGIC 
// MAGIC --

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_green_transformed_prq;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_green_transformed_prq(
// MAGIC taxi_type STRING,
// MAGIC vendor_id INT,
// MAGIC pickup_datetime TIMESTAMP,
// MAGIC dropoff_datetime TIMESTAMP,
// MAGIC store_and_fwd_flag STRING,
// MAGIC rate_code_id INT,
// MAGIC pickup_location_id INT,
// MAGIC dropoff_location_id INT,
// MAGIC pickup_longitude STRING,
// MAGIC pickup_latitude STRING,
// MAGIC dropoff_longitude STRING,
// MAGIC dropoff_latitude STRING,
// MAGIC passenger_count INT,
// MAGIC trip_distance DOUBLE,
// MAGIC fare_amount DOUBLE,
// MAGIC extra DOUBLE,
// MAGIC mta_tax DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC tolls_amount DOUBLE,
// MAGIC ehail_fee DOUBLE,
// MAGIC improvement_surcharge DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC payment_type INT,
// MAGIC trip_type INT,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING,
// MAGIC vendor_abbreviation STRING,
// MAGIC vendor_description STRING,
// MAGIC trip_type_description STRING,
// MAGIC month_name_short STRING,
// MAGIC month_name_full STRING,
// MAGIC payment_type_description STRING,
// MAGIC rate_code_description STRING,
// MAGIC pickup_borough STRING,
// MAGIC pickup_zone STRING,
// MAGIC pickup_service_zone STRING,
// MAGIC dropoff_borough STRING,
// MAGIC dropoff_zone STRING,
// MAGIC dropoff_service_zone STRING,
// MAGIC pickup_year INT,
// MAGIC pickup_month INT,
// MAGIC pickup_day INT,
// MAGIC pickup_hour INT,
// MAGIC pickup_minute INT,
// MAGIC pickup_second INT,
// MAGIC dropoff_year INT,
// MAGIC dropoff_month INT,
// MAGIC dropoff_day INT,
// MAGIC dropoff_hour INT,
// MAGIC dropoff_minute INT,
// MAGIC dropoff_second INT)
// MAGIC USING parquet
// MAGIC partitioned by (trip_year,trip_month)
// MAGIC LOCATION 'wasbs://transformed@gaiasa.blob.core.windows.net/green-taxi/';
// MAGIC 
// MAGIC --

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_all;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_all(
// MAGIC taxi_type STRING,
// MAGIC vendor_id STRING,
// MAGIC pickup_datetime TIMESTAMP,
// MAGIC dropoff_datetime TIMESTAMP,
// MAGIC store_and_fwd_flag STRING,
// MAGIC rate_code_id INT,
// MAGIC pickup_location_id INT,
// MAGIC dropoff_location_id INT,
// MAGIC pickup_longitude STRING,
// MAGIC pickup_latitude STRING,
// MAGIC dropoff_longitude STRING,
// MAGIC dropoff_latitude STRING,
// MAGIC passenger_count INT,
// MAGIC trip_distance DOUBLE,
// MAGIC fare_amount DOUBLE,
// MAGIC extra DOUBLE,
// MAGIC mta_tax DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC tolls_amount DOUBLE,
// MAGIC ehail_fee DOUBLE,
// MAGIC improvement_surcharge DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC payment_type STRING,
// MAGIC trip_type INT,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING,
// MAGIC vendor_abbreviation STRING,
// MAGIC vendor_description STRING,
// MAGIC trip_type_description STRING,
// MAGIC month_name_short STRING,
// MAGIC month_name_full STRING,
// MAGIC payment_type_description STRING,
// MAGIC rate_code_description STRING,
// MAGIC pickup_borough STRING,
// MAGIC pickup_zone STRING,
// MAGIC pickup_service_zone STRING,
// MAGIC dropoff_borough STRING,
// MAGIC dropoff_zone STRING,
// MAGIC dropoff_service_zone STRING,
// MAGIC pickup_year INT,
// MAGIC pickup_month INT,
// MAGIC pickup_day INT,
// MAGIC pickup_hour INT,
// MAGIC pickup_minute INT,
// MAGIC pickup_second INT,
// MAGIC dropoff_year INT,
// MAGIC dropoff_month INT,
// MAGIC dropoff_day INT,
// MAGIC dropoff_hour INT,
// MAGIC dropoff_minute INT,
// MAGIC dropoff_second INT)
// MAGIC USING parquet
// MAGIC partitioned by (trip_year,trip_month)
// MAGIC LOCATION 'wasbs://materialized-views@gaiasa.blob.core.windows.net/trips_all/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_hour;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_hour(
// MAGIC taxi_type STRING,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING,
// MAGIC month_name_short STRING,
// MAGIC month_name_full STRING,
// MAGIC payment_type_description STRING,
// MAGIC pickup_borough STRING,
// MAGIC dropoff_borough STRING,
// MAGIC pickup_year INT,
// MAGIC pickup_month INT,
// MAGIC pickup_day INT,
// MAGIC pickup_hour INT,
// MAGIC payment_type STRING,
// MAGIC fare_amount DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC trip_count DOUBLE)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://materialized-views@gaiasa.blob.core.windows.net/trips_hour/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_day;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_day(
// MAGIC taxi_type STRING,
// MAGIC trip_year STRING,
// MAGIC trip_month STRING,
// MAGIC month_name_short STRING,
// MAGIC month_name_full STRING,
// MAGIC payment_type_description STRING,
// MAGIC pickup_borough STRING,
// MAGIC dropoff_borough STRING,
// MAGIC pickup_year INT,
// MAGIC pickup_month INT,
// MAGIC pickup_day INT,
// MAGIC payment_type STRING,
// MAGIC fare_amount DOUBLE,
// MAGIC tip_amount DOUBLE,
// MAGIC total_amount DOUBLE,
// MAGIC trip_count DOUBLE)
// MAGIC USING parquet
// MAGIC LOCATION 'wasbs://materialized-views@gaiasa.blob.core.windows.net/trips_day/';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC --msck repair table trips_yellow_raw_prq
// MAGIC --show partitions trips_yellow_raw_prq 
// MAGIC --alter table trips_yellow_raw_prq add if not exists partition (trip_year=2016,trip_month=01) LOCATION 'wasbs://raw@gaiasa.blob.core.windows.net/yellow-taxi/trip_year=2016/trip_month=01';

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC 
// MAGIC --describe formatted trips_yellow_raw_prq;
// MAGIC --show partitions trips_yellow_raw_prq;
// MAGIC --select trip_month, count(*) from trips_yellow_raw_prq group by trip_month;
// MAGIC --REFRESH TABLE trips_yellow_raw_prq
// MAGIC --select * from trips_yellow_raw_prq where trip_year=2014 and trip_month=01

// COMMAND ----------

