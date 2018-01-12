// Databricks notebook source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType, DoubleType}
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.spark.sql.types._


// COMMAND ----------

//Schema for data
val TripGreenSchema = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("lpep_pickup_datetime", TimestampType, true),
    StructField("lpep_dropoff_datetime", TimestampType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("pulocationid", IntegerType, true),
    StructField("dolocationid", IntegerType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("ehail_fee", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("trip_type", IntegerType, true)))

// COMMAND ----------

//Read contents
val tripGreenDF = sqlContext.read.format("csv")
  .option("header", "true")
  .schema(TripGreenSchema)
  .option("delimiter",",")
  .option("mode", "DROPMALFORMED")
  .load("wasbs://nyc@gaiasa.blob.core.windows.net/test/")


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

//Create Hive external table
tripGreenDF.write.mode(SaveMode.Overwrite).saveAsTable("nyc.tripgreen")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from nyc.tripgreen where upper(vendorid) = 1

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC desc formatted nyc.tripgreen;

// COMMAND ----------

