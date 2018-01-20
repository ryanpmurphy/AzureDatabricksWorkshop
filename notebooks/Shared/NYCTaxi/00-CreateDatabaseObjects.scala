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

