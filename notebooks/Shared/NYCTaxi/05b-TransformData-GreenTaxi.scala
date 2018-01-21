// Databricks notebook source
//1. Join with lookup tables
//2. Add additional columns line hour minute time that is currently only part of date

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC select t.taxi_type,
// MAGIC   t.vendorid,
// MAGIC   t.pickup_datetime,
// MAGIC   t.dropoff_datetime,
// MAGIC   t.store_and_fwd_flag,
// MAGIC   t.ratecodeid,
// MAGIC   t.pickup_location_id,
// MAGIC   t.dropoff_location_id,
// MAGIC   t.pickup_longitude,
// MAGIC   t.pickup_latitude,
// MAGIC   t.dropoff_longitude,
// MAGIC   t.dropoff_latitude,
// MAGIC   t.passenger_count,
// MAGIC   t.trip_distance,
// MAGIC   t.fare_amount,
// MAGIC   t.extra,
// MAGIC   t.mta_tax,
// MAGIC   t.tip_amount,
// MAGIC   t.tolls_amount,
// MAGIC   t.ehail_fee,
// MAGIC   t.improvement_surcharge,
// MAGIC   t.total_amount,
// MAGIC   t.payment_type,
// MAGIC   t.trip_type,
// MAGIC   t.trip_year,
// MAGIC   t.trip_month,
// MAGIC   v.abbreviation as vendor_abbreviation,
// MAGIC   v.description as vendor_description,
// MAGIC   tt.description as trip_type_description,
// MAGIC   tm.month_name_short,
// MAGIC   tm.month_name_full,
// MAGIC   pt.description as payment_type_description,
// MAGIC   rc.description as rate_code_description,
// MAGIC   tzpu.borough as pickup_borough,
// MAGIC   tzpu.zone as pickup_zone,
// MAGIC   tzpu.service_zone as pickup_service_zone,
// MAGIC   tzdo.borough as dropoff_borough,
// MAGIC   tzdo.zone as dropoff_zone,
// MAGIC   tzdo.service_zone as dropoff_service_zone,
// MAGIC   year(t.pickup_datetime) as pickup_year,
// MAGIC   month(t.pickup_datetime) as pickup_month,
// MAGIC   day(t.pickup_datetime) as pickup_day,
// MAGIC   hour(t.pickup_datetime) as pickup_hour,
// MAGIC   minute(t.pickup_datetime) as pickup_minute,
// MAGIC   second(t.pickup_datetime) as pickup_second,
// MAGIC   date(t.pickup_datetime) as pickup_date,
// MAGIC   year(t.dropoff_datetime) as dropoff_year,
// MAGIC   month(t.dropoff_datetime) as dropoff_month,
// MAGIC   day(t.dropoff_datetime) as dropoff_day,
// MAGIC   hour(t.dropoff_datetime) as dropoff_hour,
// MAGIC   minute(t.dropoff_datetime) as dropoff_minute,
// MAGIC   second(t.dropoff_datetime) as dropoff_second,
// MAGIC   date(t.dropoff_datetime) as dropoff_date
// MAGIC from trips_green_raw_prq t
// MAGIC join refdata_vendor_lookup v on t.vendorid = v.vendorid
// MAGIC join refdata_trip_type_lookup tt on t.trip_type = tt.trip_type
// MAGIC join refdata_trip_month_lookup tm on t.trip_month = tm.trip_month
// MAGIC join refdata_payment_type_lookup pt on t.payment_type = pt.payment_type
// MAGIC left join refdata_rate_code_lookup rc on t.ratecodeid = rc.ratecodeid
// MAGIC left join refdata_taxi_zone_lookup tzpu on t.pickup_location_id = tzpu.location_id
// MAGIC left join refdata_taxi_zone_lookup tzdo on t.dropoff_location_id = tzdo.location_id
// MAGIC ;