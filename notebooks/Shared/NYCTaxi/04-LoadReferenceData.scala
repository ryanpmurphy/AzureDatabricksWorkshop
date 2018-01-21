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

//3.  Rate code id lookup
//Applicable for: yellow taxi, greentaxi?
val rateCodeSchema = StructType(Array(
    StructField("ratecodeid", IntegerType, true),
    StructField("description", StringType, true)))

//4.  Payment type lookup
//Applicable for: yellow taxi, green taxi
val paymentTypeSchema = StructType(Array(
    StructField("payment_type", IntegerType, true),
    StructField("description", StringType, true)))

//5. Trip type
//Applicable for: yellow taxi?, green taxi
val tripTypeSchema = StructType(Array(
    StructField("trip_type", IntegerType, true),
    StructField("description", StringType, true)))


//6. Vendor ID
//Applicable for: yellow taxi?, green taxi?
val vendorSchema = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("abbreviation", StringType, true),
    StructField("description", StringType, true)))




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
val refDF = sqlContext.read.option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .csv(srcDataFile)
      
//Write parquet output
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
val refDF = sqlContext.read.option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .csv(srcDataFile)
      
//Write parquet output
refDF.coalesce(1).write.parquet(destDataDir)

println("Saved data")

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

//Refresh table
sql("REFRESH TABLE nyc_db.refdata_trip_month_lookup")

// COMMAND ----------

//.............................................................
// 3.  LOAD RATE CODE
//.............................................................

//Execute for idempotent runs
dbutils.fs.rm(destDataDirRoot + "rate-code", recurse=true)

//Source path  
val srcDataFile= srcDataDirRoot + "rate_code_lookup.csv"
println("srcDataFile=" + srcDataFile)

//Destination path  
val destDataDir = destDataDirRoot + "rate-code"
println("destDataDir=" + destDataDir)
      
//Source schema
val srcSchema = rateCodeSchema

//Read source data
val refDF = sqlContext.read.option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .csv(srcDataFile)
      
//Write parquet output
refDF.coalesce(1).write.parquet(destDataDir)

println("Saved data")

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

// Refresh table
sql("REFRESH TABLE nyc_db.refdata_rate_code_lookup")

// COMMAND ----------

//.............................................................
// 4.  LOAD PAYMENT TYPE
//.............................................................

//Execute for idempotent runs
dbutils.fs.rm(destDataDirRoot + "payment-type", recurse=true)

//Source path  
val srcDataFile= srcDataDirRoot + "payment_type_lookup.csv"
println("srcDataFile=" + srcDataFile)

//Destination path  
val destDataDir = destDataDirRoot + "payment-type"
println("destDataDir=" + destDataDir)
      
//Source schema
val srcSchema = paymentTypeSchema

//Read source data
val refDF = sqlContext.read.option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .csv(srcDataFile)
      
//Write parquet output
refDF.coalesce(1).write.parquet(destDataDir)

println("Saved data")

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

//Refresh table
sql("REFRESH TABLE nyc_db.refdata_payment_type_lookup")

// COMMAND ----------

//.............................................................
// 5.  LOAD TRIP TYPE
//.............................................................

//Execute for idempotent runs
dbutils.fs.rm(destDataDirRoot + "trip-type", recurse=true)

//Source path  
val srcDataFile= srcDataDirRoot + "trip_type_lookup.csv"
println("srcDataFile=" + srcDataFile)

//Destination path  
val destDataDir = destDataDirRoot + "trip-type"
println("destDataDir=" + destDataDir)
      
//Source schema
val srcSchema = tripTypeSchema

//Read source data
val refDF = sqlContext.read.option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .csv(srcDataFile)
      
//Write parquet output
refDF.coalesce(1).write.parquet(destDataDir)

println("Saved data")

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

//Refresh table
sql("REFRESH TABLE nyc_db.refdata_trip_type_lookup")

// COMMAND ----------

//.............................................................
// 5.  LOAD VENDOR
//.............................................................

//Execute for idempotent runs
dbutils.fs.rm(destDataDirRoot + "vendor", recurse=true)

//Source path  
val srcDataFile= srcDataDirRoot + "vendor_lookup.csv"
println("srcDataFile=" + srcDataFile)

//Destination path  
val destDataDir = destDataDirRoot + "vendor"
println("destDataDir=" + destDataDir)
      
//Source schema
val srcSchema = vendorSchema

//Read source data
val refDF = sqlContext.read.option("header", "true")
                      .schema(srcSchema)
                      .option("delimiter",",")
                      .csv(srcDataFile)

//Write parquet output
refDF.coalesce(1).write.parquet(destDataDir)

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls(destDataDir + "/").foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

//Refresh table
sql("REFRESH TABLE nyc_db.refdata_vendor_lookup")

// COMMAND ----------

// %sql
// select * from nyc_db.refdata_vendor_lookup;

// COMMAND ----------

