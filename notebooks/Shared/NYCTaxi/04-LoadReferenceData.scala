// Databricks notebook source
//LOAD REFERENCE DATA

// COMMAND ----------

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import spark.implicits._
import spark.sql

// COMMAND ----------

//1.  Taxi zone lookup
//Applicable for: Yellow taxi, greentaxi?
val taxiZoneSchema = StructType(Array(
    StructField("location_id", StringType, true),
    StructField("borough", StringType, true),
    StructField("zone", StringType, true),
    StructField("service_zone", StringType, true)))

//2. Months of the year
//Applicable for: yellow taxi, green taxi
val monthNameSchema = StructType(Array(
    StructField("trip_month", StringType, true),
    StructField("month_name_short", StringType, true),
    StructField("month_name_full", StringType, true)))

//2.  Rate code id lookup
//Applicable for: yellow taxi, greentaxi?

//3.  Payment type lookup
//Applicable for: yellow taxi, green taxi

//4. Trip type
//Applicable for: yellow taxi?, green taxi


//5. Vendor ID
//Applicable for: yellow taxi?, green taxi?




// COMMAND ----------

val fs = FileSystem.get(new Configuration())
val srcDataDirRoot = "/mnt/data/nyctaxi/source/refdata/" //Root dir for source data
val destDataDirRoot = "/mnt/data/nyctaxi/refdata/" //Root dir for consumable data


// COMMAND ----------

//.............................................................
// 1.  LOAD TAXI-ZONE
//.............................................................

//Execute for idempotent runs
dbutils.fs.rm(destDataDirRoot + "taxi-zone", recurse=true)

//Source path  
val srcDataFile= srcDataDirRoot + "taxi_zone_lookup.csv"
println("srcDataFile=" + srcDataFile)

//Destination path  
val destDataDir = destDataDirRoot + "taxi-zone"
println("destDataDir=" + destDataDir)
      
//Source schema
val srcSchema = taxiZoneSchema

//Read source data
val refDF = sqlContext.read.format("csv")
                      .option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .load(srcDataFile)
      
//Write parquet output, calling function to calculate number of partition files
refDF.coalesce(1).write.parquet(destDataDir)

println("Saved data")

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

//Refresh table
sql("REFRESH TABLE nyc_db.refdata_taxi_zone_lookup")

// COMMAND ----------

//.............................................................
// 2.  LOAD MONTH
//.............................................................

//Execute for idempotent runs
dbutils.fs.rm(destDataDirRoot + "trip-month", recurse=true)

//Source path  
val srcDataFile= srcDataDirRoot + "trip_month_lookup.csv"
println("srcDataFile=" + srcDataFile)

//Destination path  
val destDataDir = destDataDirRoot + "trip-month"
println("destDataDir=" + destDataDir)
      
//Source schema
val srcSchema = monthNameSchema

//Read source data
val refDF = sqlContext.read.format("csv")
                      .option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .load(srcDataFile)
      
//Write parquet output, calling function to calculate number of partition files
refDF.coalesce(1).write.parquet(destDataDir)

println("Saved data")

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

//Refresh table
//sql("REFRESH TABLE nyc_db.refdata_trip_month_lookup")

// COMMAND ----------

