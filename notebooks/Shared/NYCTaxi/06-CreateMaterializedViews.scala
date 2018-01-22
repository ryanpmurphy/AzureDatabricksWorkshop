// Databricks notebook source
//Join green and yellow at needed granularity for each materialized view

// COMMAND ----------

// %sql
// --All trips
// select * from 
//   (
//   select taxi_type,
//     cast(vendor_id as string) as vendor_id,
//     pickup_datetime,
//     dropoff_datetime,
//     store_and_fwd_flag,
//     rate_code_id,
//     pickup_location_id,
//     dropoff_location_id,
//     pickup_longitude,
//     pickup_latitude,
//     dropoff_longitude,
//     dropoff_latitude,
//     passenger_count,
//     trip_distance,
//     fare_amount,
//     extra,
//     mta_tax,
//     tip_amount,
//     tolls_amount,
//     ehail_fee,
//     improvement_surcharge,
//     total_amount,
//     cast(payment_type as string) as payment_type,
//     trip_type,
//     trip_year,
//     trip_month,
//     vendor_abbreviation,
//     vendor_description,
//     trip_type_description,
//     month_name_short,
//     month_name_full,
//     payment_type_description,
//     rate_code_description,
//     pickup_borough,
//     pickup_zone,
//     pickup_service_zone,
//     dropoff_borough,
//     dropoff_zone,
//     dropoff_service_zone,
//     pickup_year,
//     pickup_month,
//     pickup_day,
//     pickup_hour,
//     pickup_minute,
//     pickup_second,
//     dropoff_year,
//     dropoff_month,
//     dropoff_day,
//     dropoff_hour,
//     dropoff_minute,
//     dropoff_second
//   from nyc_db.trips_green_transformed_prq

//   union all

//   select taxi_type,
//     vendor_id,
//     pickup_datetime,
//     dropoff_datetime,
//     store_and_fwd_flag,
//     rate_code_id,
//     pickup_location_id,
//     dropoff_location_id,
//     pickup_longitude,
//     pickup_latitude,
//     dropoff_longitude,
//     dropoff_latitude,
//     passenger_count,
//     trip_distance,
//     fare_amount,
//     extra,
//     mta_tax,
//     tip_amount,
//     tolls_amount,
//     null as ehail_fee,
//     improvement_surcharge,
//     total_amount,
//     payment_type,
//     null as trip_type,
//     trip_year,
//     trip_month,
//     vendor_abbreviation,
//     vendor_description,
//     null as trip_type_description,
//     month_name_short,
//     month_name_full,
//     payment_type_description,
//     rate_code_description,
//     pickup_borough,
//     pickup_zone,
//     pickup_service_zone,
//     dropoff_borough,
//     dropoff_zone,
//     dropoff_service_zone,
//     pickup_year,
//     pickup_month,
//     pickup_day,
//     pickup_hour,
//     pickup_minute,
//     pickup_second,
//     dropoff_year,
//     dropoff_month,
//     dropoff_day,
//     dropoff_hour,
//     dropoff_minute,
//     dropoff_second
//   from nyc_db.trips_yellow_transformed_prq
//   ) a

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_by_hour;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS trips_by_hour(
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
// MAGIC LOCATION 'wasbs://materialized-views@gaiasa.blob.core.windows.net/trips_by_hour/';

// COMMAND ----------

val destDataDirRoot = "/mnt/data/nyctaxi/materialized-views" //Root dir for transformed data

// COMMAND ----------

//Delete any residual data from prior executions for an idempotent run
val dataDir=destDataDirRoot + "/trips_by_hour"
val deleteDirStatus = dbutils.fs.rm(dataDir,recurse=true)
println(deleteDirStatus)

// COMMAND ----------

//Read transformed data from database tables
//save as parquet
//refresh table
//compute table statistics

//Read transformed data
//Aggregate transformed trips
val sqlDF = spark.sql("""
  select 
    taxi_type,
    trip_year,
    trip_month,
    month_name_short,
    month_name_full,
    payment_type_description,
    pickup_borough,
    dropoff_borough,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    cast(payment_type as string) as payment_type,
    sum(fare_amount) as fare_amount,
    sum(tip_amount) as tip_amount,
    sum(total_amount) as total_amount,
    count(*) as trip_count
  from nyc_db.trips_green_transformed_prq
  group by taxi_type,
    trip_year,
    trip_month,
    month_name_short,
    month_name_full,
    payment_type_description,
    pickup_borough,
    dropoff_borough,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    payment_type
  
  union all
  
  select 
    taxi_type,
    trip_year,
    trip_month,
    month_name_short,
    month_name_full,
    payment_type_description,
    pickup_borough,
    dropoff_borough,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    payment_type,
    sum(fare_amount) as fare_amount,
    sum(tip_amount) as tip_amount,
    sum(total_amount) as total_amount,
    count(*) as trip_count
  from nyc_db.trips_yellow_transformed_prq
  group by taxi_type,
    trip_year,
    trip_month,
    month_name_short,
    month_name_full,
    payment_type_description,
    pickup_borough,
    dropoff_borough,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    payment_type
  """)

//Write parquet output, calling function to calculate number of partition files
sqlDF.coalesce(1).write.parquet(destDataDirRoot + "/trips_by_hour")


//Refresh table
sql("REFRESH TABLE nyc_db.trips_by_hour")

//Compute table statistics
sql("ANALYZE TABLE nyc_db.trips_by_hour COMPUTE STATISTICS")


// COMMAND ----------

// MAGIC %sql
// MAGIC --ANALYZE TABLE nyc_db.trips_by_hour COMPUTE STATISTICS
// MAGIC --  REFRESH TABLE nyc_db.trips_by_hour
// MAGIC 
// MAGIC select
// MAGIC taxi_type,
// MAGIC trip_year,
// MAGIC trip_month,
// MAGIC month_name_short,
// MAGIC month_name_full,
// MAGIC payment_type_description,
// MAGIC pickup_borough,
// MAGIC dropoff_borough,
// MAGIC pickup_year,
// MAGIC pickup_month,
// MAGIC pickup_day,
// MAGIC pickup_hour,
// MAGIC  payment_type,
// MAGIC  fare_amount,
// MAGIC tip_amount,
// MAGIC total_amount
// MAGIC trip_count
// MAGIC from nyc_db.trips_by_hour

// COMMAND ----------

