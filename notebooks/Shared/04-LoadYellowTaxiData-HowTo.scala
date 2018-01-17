// Databricks notebook source
// MAGIC %md
// MAGIC This is a "How to"

// COMMAND ----------

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType}
import com.databricks.backend.daemon.dbutils.FileInfo

// COMMAND ----------

// Variables
val srcDataFile = "/mnt/data/nycdata/source/year=2017/month=06/type=yellow/yellow_tripdata_2017-06.csv"
val destDataDir = "/mnt/data/nycdata/raw/nyc/year=2017/month=06/"

// COMMAND ----------

// ====================================================================
// LOAD YEAR 2017 YELLOW TAXI DATA
// ====================================================================

//c(1) Review the file to see if inferSchema can be leveraged
dbutils.fs.head(srcDataFile)

// COMMAND ----------

// (2) Read into dataframe, leverage inferSchema, and drop malformed data
val yellowTaxi2017DF = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema","true")
  .option("delimiter",",")
  .option("mode", "DROPMALFORMED")
  .load(srcDataFile).cache()

// COMMAND ----------

// (3) Review schema to see if columns need renaming
yellowTaxi2017DF.printSchema

// COMMAND ----------

// (4) Rename columns
val yellowTaxi2017ReformattedDF = yellowTaxi2017DF.select($"VendorID" as "vendor_id",
                                    $"tpep_pickup_datetime" as "pickup_datetime", 
                                    $"tpep_dropoff_datetime" as "dropoff_datetime", 
                                    $"passenger_count", 
                                    $"trip_distance",
                                    $"RatecodeID" as "rate_code_id",
                                    $"store_and_fwd_flag",
                                    $"PULocationID" as "pickup_location_id",
                                    $"DOLocationID" as "dropoff_location_id",
                                    $"payment_type",
                                    $"fare_amount" ,
                                    $"extra",
                                    $"mta_tax" ,
                                    $"tip_amount",
                                    $"tolls_amount",
                                    $"improvement_surcharge",
                                    $"total_amount"
                                    )

// COMMAND ----------

// (5) Add additional columns like year, month and taxi type
import org.apache.spark.sql.functions._
val yellowTaxi2017DFWithMetadata = yellowTaxi2017ReformattedDF.withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                                                              .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                                                              .withColumn("taxi_type",lit("yellow"))
                                                              .withColumn("pickup_longitude", lit(""))
                                                              .withColumn("pickup_latitude", lit(""))
                                                              .withColumn("dropoff_longitude", lit(""))
                                                              .withColumn("dropoff_latitude", lit(""))
                                                                         

// COMMAND ----------

// (6) Review schema tpost renaming and adding new 
yellowTaxi2017DFWithMetadata.printSchema

// COMMAND ----------

// (7) Count rows
yellowTaxi2017ReformattedDF.count()
//9656993

// COMMAND ----------

// (8) Take a peek at what the data looks like
yellowTaxi2017DFWithMetadata.head()

// COMMAND ----------

// (9) In case you want to save as managed table
// Storage account credentials need to be saved at cluster level

//yellowTaxi2017DFWithMetadata.write.mode(SaveMode.Overwrite).saveAsTable("default.tripyellow")

// COMMAND ----------

// (10) Create destination directory
dbutils.fs.mkdirs(destDataDir)

// COMMAND ----------

// (11) Clear the directory of any contents if it already existed prior to step 10
// ADB does not support regex/wildcards
dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => {
      val filename = i.path  
      val delStatus = dbutils.fs.rm(filename)
      println("Status of deletion of " + filename + " = " + delStatus)
  })

// COMMAND ----------

// (12) Determine how many output files to coalesce to
// In our trails, we found that 812 MB was reduced to 150 MB = 3/16 - we will use this

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration

val fs = FileSystem.get(new Configuration())
val rawSize= fs.getContentSummary(new Path(srcDataFile)).getLength 
val inputDirSize = fs.getContentSummary(new Path(srcDataFile)).getLength * 3/16
val minCompactedFileSizeInMB = 64
var outputFileCount = Math.floor(inputDirSize / (minCompactedFileSizeInMB * 1024 * 1024)).toInt
if (outputFileCount == 0) outputFileCount = 1

println("rawSize=" + rawSize)
println("inputDirSize=" + inputDirSize)
println("outputFileCount=" + outputFileCount)

// COMMAND ----------

// (13) Persist to parquet - into raw directory
yellowTaxi2017DFWithMetadata.coalesce(outputFileCount).write.mode(SaveMode.Overwrite).parquet(destDataDir)
//yellowTaxi2017DFWithMetadata.coalesce(outputFileCount).write.partitionBy("trip_year").mode(SaveMode.Overwrite).parquet(destDataDir)

// COMMAND ----------

// (14) List contents
display(dbutils.fs.ls(destDataDir))

// COMMAND ----------

// (15) Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => {
  if ((i.path contains "_"))
  {
      val targetFilename = i.path
      val delStatus = dbutils.fs.rm(targetFilename)
      println("Status of deletion of " + targetFilename + " = " + delStatus)
  }})


// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS trips_yellow;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS nyc_db.trips_yellow(vendor_id INT,pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, passenger_count INT, trip_distance DOUBLE, RatecodeID INT, store_and_fwd_flag STRING, pickup_location_id INT, dropoff_location_id INT, payment_type INT, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE, year STRING, month STRING, taxi_type STRING) USING parquet
// MAGIC LOCATION 'wasbs://raw@gaialabsa.blob.core.windows.net/nyc/year=2017/month=06';

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC use nyc_db;
// MAGIC show tables;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC use nyc_db;
// MAGIC select sum(passenger_count) from trips_yellow;

// COMMAND ----------

// (16) IN REALITY - we will partition the results by year and month dynamically using Spark partitioning base don dataframe column
// So we clean up all our work now that we know how to load


dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => {
      val filename = i.path  
      val delStatus = dbutils.fs.rm(filename)
      println("Status of deletion of " + filename + " = " + delStatus)
  })

// COMMAND ----------

dbutils.fs.rm("/mnt/data/nycdata/raw/nyc/year=2017/", recurse=true)

// COMMAND ----------

dbutils.fs.ls("/mnt/data/nycdata/raw/nyc/")

// COMMAND ----------

