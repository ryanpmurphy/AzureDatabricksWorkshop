// Databricks notebook source
import spark.implicits._
import spark.sql
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

// COMMAND ----------

val fs = FileSystem.get(new Configuration())
val destDataDirRoot = "/mnt/data/nyctaxi/transformed/yellow-taxi" //Root dir for transformed data

// COMMAND ----------

//Delete any residual data from prior executions for an idempotent run
val dataDir=destDataDirRoot
val deleteDirStatus = dbutils.fs.rm(dataDir,recurse=true)
println(deleteDirStatus)

// COMMAND ----------

// Recursive file listing. Must have with partitioned table output.
def allFiles(path: String): Seq[String] =
  dbutils.fs.ls(path).map(file => {
    // Work around double encoding bug
     val path = file.path.replace("%25", "%").replace("%25", "%")
    if (file.isDir) allFiles(path)
    else Seq[String](path)
  }).reduce(_ ++ _)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC --Refresh raw table
// MAGIC REFRESH TABLE nyc_db.trips_yellow_raw_prq;
// MAGIC 
// MAGIC -- REFRESH TABLE nyc_db.refdata_vendor_lookup;
// MAGIC -- REFRESH TABLE nyc_db.refdata_trip_type_lookup;
// MAGIC -- REFRESH TABLE nyc_db.refdata_trip_month_lookup;
// MAGIC -- REFRESH TABLE nyc_db.refdata_payment_type_lookup;
// MAGIC -- REFRESH TABLE nyc_db.refdata_rate_code_lookup;
// MAGIC -- REFRESH TABLE nyc_db.refdata_taxi_zone_lookup

// COMMAND ----------

//Read raw data from database tables
//save as parquet

//Read raw data
//Join raw trips to reference data tables, and add time element columns
val sqlDF = spark.sql("""
  select t.taxi_type,
      t.vendor_id,
      t.pickup_datetime,
      t.dropoff_datetime,
      t.store_and_fwd_flag,
      t.rate_code_id,
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
      t.improvement_surcharge,
      t.total_amount,
      t.payment_type,
      t.trip_year,
      t.trip_month,
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
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
  from nyc_db.trips_yellow_raw_prq t
  left join nyc_db.refdata_vendor_lookup v on t.vendor_id = case when t.trip_year < "2015" then v.abbreviation else v.vendor_id end
  left join nyc_db.refdata_trip_month_lookup tm on t.trip_month = tm.trip_month
  left join nyc_db.refdata_payment_type_lookup pt on t.payment_type = case when t.trip_year < "2015" then pt.abbreviation else pt.payment_type end
  left join nyc_db.refdata_rate_code_lookup rc on t.rate_code_id = rc.rate_code_id
  left join nyc_db.refdata_taxi_zone_lookup tzpu on t.pickup_location_id = tzpu.location_id
  left join nyc_db.refdata_taxi_zone_lookup tzdo on t.dropoff_location_id = tzdo.location_id
  """)

//Write parquet output, calling function to calculate number of partition files
sqlDF.write.partitionBy("trip_year", "trip_month").parquet(destDataDirRoot)



// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
allFiles(destDataDirRoot).foreach((path) => {
  if (path.indexOf("parquet") == -1)
  {
      val delStatus = dbutils.fs.rm(path)
      println("Status of deletion of " + path + " = " + delStatus)
  }})


// COMMAND ----------

//Add partitions to transformed table

for (j <- 2009 to 2017)
  {
    val monthsCount = if (j==2017) 6 else 12 
    for (i <- 1 to monthsCount) 
    {    
      val destDataDir = destDataDirRoot + "/trip_year=" + j + "/trip_month=" + "%02d".format(i) + "/"

      //Add partition for year and month
      val alterTableSql = "ALTER TABLE nyc_db.trips_yellow_transformed_prq ADD IF NOT EXISTS PARTITION (trip_year=" + j + ",trip_month=" + "%02d".format(i) + ") LOCATION '" + destDataDir.dropRight(1) + "'"
      sql(alterTableSql)
      //println(alterTableSql)
      
       //Refresh table
       sql("REFRESH TABLE nyc_db.trips_yellow_transformed_prq")
    }
  }
sql("ANALYZE TABLE nyc_db.trips_yellow_transformed_prq COMPUTE STATISTICS")


// COMMAND ----------

// %sql
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
// --     ehail_fee,
//     improvement_surcharge,
//     total_amount,
//     payment_type,
// --     trip_type,
//     trip_year,
//     trip_month,
//     vendor_abbreviation,
//     vendor_description,
// --     trip_type_description,
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


// COMMAND ----------

