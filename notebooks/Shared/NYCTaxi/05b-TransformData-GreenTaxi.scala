// Databricks notebook source
//1. Join with lookup tables
//2. Add additional columns line hour minute time that is currently only part of date

// COMMAND ----------

dbutils.fs.rm("/mnt/data/nyctaxi/transformed/green-taxi/",recurse=true)

// COMMAND ----------

sql("REFRESH TABLE nyc_db.trips_green_raw_prq")

// COMMAND ----------

val sqlDF = spark.sql("""
  select t.taxi_type,
      t.vendorid,
      t.pickup_datetime,
      t.dropoff_datetime,
      t.store_and_fwd_flag,
      t.ratecodeid,
      t.pickup_location_id,
      t.dropoff_location_id,
      t.pickup_longitude,
      t.pickup_latitude,
      t.dropoff_longitude,
      t.dropoff_latitude,
      t.passenger_count,
      t.trip_distance,
      t.fare_amount,
      t.extra,
      t.mta_tax,
      t.tip_amount,
      t.tolls_amount,
      t.ehail_fee,
      t.improvement_surcharge,
      t.total_amount,
      t.payment_type,
      t.trip_type,
      t.trip_year,
      t.trip_month,
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
      tt.description as trip_type_description,
      tm.month_name_short,
      tm.month_name_full,
      pt.description as payment_type_description,
      rc.description as rate_code_description,
      tzpu.borough as pickup_borough,
      tzpu.zone as pickup_zone,
      tzpu.service_zone as pickup_service_zone,
      tzdo.borough as dropoff_borough,
      tzdo.zone as dropoff_zone,
      tzdo.service_zone as dropoff_service_zone,
      year(t.pickup_datetime) as pickup_year,
      month(t.pickup_datetime) as pickup_month,
      day(t.pickup_datetime) as pickup_day,
      hour(t.pickup_datetime) as pickup_hour,
      minute(t.pickup_datetime) as pickup_minute,
      second(t.pickup_datetime) as pickup_second,
      date(t.pickup_datetime) as pickup_date,
      year(t.dropoff_datetime) as dropoff_year,
      month(t.dropoff_datetime) as dropoff_month,
      day(t.dropoff_datetime) as dropoff_day,
      hour(t.dropoff_datetime) as dropoff_hour,
      minute(t.dropoff_datetime) as dropoff_minute,
      second(t.dropoff_datetime) as dropoff_second,
      date(t.dropoff_datetime) as dropoff_date
  from nyc_db.trips_green_raw_prq t
  join nyc_db.refdata_vendor_lookup v on t.vendorid = v.vendorid
  join nyc_db.refdata_trip_type_lookup tt on t.trip_type = tt.trip_type
  join nyc_db.refdata_trip_month_lookup tm on t.trip_month = tm.trip_month
  join nyc_db.refdata_payment_type_lookup pt on t.payment_type = pt.payment_type
  left join nyc_db.refdata_rate_code_lookup rc on t.ratecodeid = rc.ratecodeid
  left join nyc_db.refdata_taxi_zone_lookup tzpu on t.pickup_location_id = tzpu.location_id
  left join nyc_db.refdata_taxi_zone_lookup tzdo on t.dropoff_location_id = tzdo.location_id
  """)
//sqlDF.show()

sqlDF.write.partitionBy("trip_year", "trip_month").parquet("/mnt/data/nyctaxi/transformed/green-taxi/")

// COMMAND ----------

