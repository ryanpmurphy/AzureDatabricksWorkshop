// Databricks notebook source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType, DoubleType}
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.spark.sql.types._


// COMMAND ----------

//Schema for data
val TripYellowSchema = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("pulocationid", IntegerType, true),
    StructField("dolocationid", IntegerType, true),
    StructField("payment_type", IntegerType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", IntegerType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true)))

// COMMAND ----------

//Read contents
val tripYellowDF = sqlContext.read.format("csv")
  .option("header", "true")
  .schema(TripYellowSchema)
  .option("delimiter",",")
  .option("mode", "DROPMALFORMED")
  .load("wasbs://nyc@gaiasa.blob.core.windows.net/test/yellow/")


// COMMAND ----------

tripGreenDF.count

// COMMAND ----------

//Print schema
tripGreenDF.printSchema()

// COMMAND ----------

//Print contents
display(tripGreenDF.take(20))

// COMMAND ----------

//Delete files from prior execution, if any
dbutils.fs.ls("wasbs://raw@gaiasa.blob.core.windows.net/test/").foreach((i: FileInfo) => {
      val filename = i.path  
      val delStatus = dbutils.fs.rm(filename)
      println("Status of deletion of " + filename + " = " + delStatus)
  })

// COMMAND ----------

//Save dataframe in parquet to raw storage directory
tripGreenDF.write.mode(SaveMode.Overwrite).parquet("wasbs://raw@gaiasa.blob.core.windows.net/test/")

// COMMAND ----------

//List output
display(dbutils.fs.ls("wasbs://raw@gaiasa.blob.core.windows.net/test/"))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*)
dbutils.fs.ls("wasbs://raw@gaiasa.blob.core.windows.net/test/").foreach((i: FileInfo) => {
  if ((i.path contains "_"))
  {
      val filename = i.path
      val delStatus = dbutils.fs.rm(filename)
      println("Status of deletion of " + filename + " = " + delStatus)
  }})

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS nyc_raw.tripgreen (vendorid INT, lpep_pickup_datetime TIMESTAMP, lpep_dropoff_datetime TIMESTAMP, store_and_fwd_flag STRING, ratecodeid INT, pulocationid INT, dolocationid INT, passenger_count INT, trip_distance DOUBLE, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, ehail_fee DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE, payment_type INT, trip_type INT) USING parquet
// MAGIC LOCATION 'wasbs://raw@gaiasa.blob.core.windows.net/test/';
// MAGIC 
// MAGIC REFRESH TABLE nyc_raw.tripgreen;

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_raw;
// MAGIC select * from nyc.tripgreen where upper(vendorid) = 1;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC desc formatted nyc_raw.tripgreen;

// COMMAND ----------

