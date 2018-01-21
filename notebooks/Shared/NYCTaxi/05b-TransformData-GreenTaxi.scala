// Databricks notebook source
import spark.implicits._
import spark.sql
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

// COMMAND ----------

val fs = FileSystem.get(new Configuration())
val destDataDirRoot = "/mnt/data/nyctaxi/transformed/green-taxi" //Root dir for transformed data

// COMMAND ----------

//Delete any residual data from prior executions for an idempotent run
val dataDir=destDataDirRoot
val deleteDirStatus = dbutils.fs.rm(dataDir,recurse=true)
println(deleteDirStatus)

// COMMAND ----------

//Read raw data from database tables
//save as parquet

//Read raw data
//Join raw trips to reference data tables, and add time element columns
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
  left join nyc_db.refdata_vendor_lookup v on t.vendorid = v.vendorid
  left join nyc_db.refdata_trip_type_lookup tt on t.trip_type = tt.trip_type
  left join nyc_db.refdata_trip_month_lookup tm on t.trip_month = tm.trip_month
  left join nyc_db.refdata_payment_type_lookup pt on t.payment_type = pt.payment_type
  left join nyc_db.refdata_rate_code_lookup rc on t.ratecodeid = rc.ratecodeid
  left join nyc_db.refdata_taxi_zone_lookup tzpu on t.pickup_location_id = tzpu.location_id
  left join nyc_db.refdata_taxi_zone_lookup tzdo on t.dropoff_location_id = tzdo.location_id
  """)

//Write parquet output, calling function to calculate number of partition files
sqlDF.write.partitionBy("trip_year", "trip_month").parquet(destDataDirRoot)

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
//dbutils.fs.ls(destDataDirRoot).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))


// //Refresh table
// sql("REFRESH TABLE nyc_db.trips_green_raw_prq")

// sql("ANALYZE TABLE nyc_db.trips_green_raw_prq COMPUTE STATISTICS")


// COMMAND ----------

//Check if file exists - Test
//val exists = fs.exists(new Path("/mnt/data/nyctaxi/source/year=2016/month=01/type=green/"))


// COMMAND ----------

//dbutils.fs.head("/mnt/data/nyctaxi/source/year=2016/month=01/type=green/green_tripdata_2016-01.csv")

// COMMAND ----------

