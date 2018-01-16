// Databricks notebook source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType, DoubleType}
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.spark.sql.types._


// COMMAND ----------

//Schema for data
val TripGreenSchema2017H1 = StructType(Array(
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

val TripGreenSchema2016H2 = StructType(Array(
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
    StructField("trip_type", IntegerType, true),
    StructField("junk1", StringType, true),
    StructField("junk2", StringType, true)))

val TripGreenSchema2015H22016H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("lpep_pickup_datetime", TimestampType, true),
    StructField("lpep_dropoff_datetime", TimestampType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
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

val TripGreenSchema2015H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("lpep_pickup_datetime", TimestampType, true),
    StructField("lpep_dropoff_datetime", TimestampType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
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
    StructField("trip_type", IntegerType, true),
    StructField("junk1", StringType, true),
    StructField("junk2", StringType, true)))

val TripGreenSchemaPre2015 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("lpep_pickup_datetime", TimestampType, true),
    StructField("lpep_dropoff_datetime", TimestampType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("ehail_fee", DoubleType, true),
    //StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("trip_type", IntegerType, true),
    StructField("junk1", StringType, true),
    StructField("junk2", StringType, true)))

val TripYellowSchema2017H1 = StructType(Array(
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
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true)))

val TripYellowSchema2016H2 = StructType(Array(
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
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("junk1", StringType, true),
    StructField("junk2", StringType, true)))

val TripYellowSchema20152016H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true)))

val TripYellowSchemaPre2015 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("total_amount", DoubleType, true)))

// COMMAND ----------

//Set values
val Type = "yellow"
val Schema = TripYellowSchema2016H2
val Year = "2016"
val Month = "05"

val FileSizeMB = 64

// COMMAND ----------

//Load dataframe from source data
val tripDF = sqlContext.read.format("csv")
  .option("header", "true")
  .schema(Schema)
  .option("delimiter",",")
  .option("mode", "DROPMALFORMED")
  .load("wasbs://nyc@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/" + Type + "_tripdata_" + Year + "-" + Month + ".csv")

// COMMAND ----------

//Delete files from prior execution, if any
dbutils.fs.ls("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/").foreach((i: FileInfo) => {
      val filename = i.path  
      val delStatus = dbutils.fs.rm(filename)
      println("Status of deletion of " + filename + " = " + delStatus)
  })

// COMMAND ----------

//Save dataframe in parquet to raw storage directory
tripDF.write.mode(SaveMode.Overwrite).parquet("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/")


// COMMAND ----------

//Re-size partition files
var totalpartfilecount : Int = 0
var totalsize : Long = 0

dbutils.fs.ls("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/").foreach((i: FileInfo) => {
  totalpartfilecount = totalpartfilecount + 1
  totalsize = totalsize + i.size
})

val roundedpartfilecount = (totalsize / 1024 / 1024 / FileSizeMB)
if((totalsize / 1024 / 1024 % FileSizeMB) > 0.0) //remainder file
{
  totalpartfilecount = (roundedpartfilecount + 1).toInt
}

println("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/")
println(totalsize)
//tripDF.coalesce(totalpartfilecount).write.mode(SaveMode.Overwrite).parquet("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/")


// COMMAND ----------

// display(tripDF.take(20))

// COMMAND ----------


// dbutils.fs.ls("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/").foreach((i: FileInfo) => {

//   val roundedpartfilecount = i.size / 1024 / 1024 / FileSizeMB //rounded down
//   var totalpartfilecount : Int = 0
//   val increment : Int = 0
//   if((i.size / 1024 / 1024 % FileSizeMB) > 0.0) //remainder file
//   {
//     totalpartfilecount = (roundedpartfilecount + 1).toInt
//   }

  
//   //tripDF.coalesce(totalpartfilecount).write.mode(SaveMode.Overwrite).parquet("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/")
// println(totalpartfilecount)
// })


// COMMAND ----------

//Count rows in dataframe
tripDF.count

// COMMAND ----------

//Print schema
tripDF.printSchema()

// COMMAND ----------

//List output
display(dbutils.fs.ls("wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/"))

// COMMAND ----------

//Delete residual files from job operation (_SUCCESS, _start*, _committed*) dbut ...

// COMMAND ----------

// val HivePath = "wasbs://raw@gaiasa.blob.core.windows.net/year=" + Year + "/month=" + Month + "/type=" + Type + "/"
// println(HivePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS nyc_raw.tripgreen (vendorid INT, lpep_pickup_datetime TIMESTAMP, lpep_dropoff_datetime TIMESTAMP, store_and_fwd_flag STRING, ratecodeid INT, pulocationid INT, dolocationid INT, passenger_count INT, trip_distance DOUBLE, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, ehail_fee DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE, payment_type INT, trip_type INT) USING parquet
// MAGIC LOCATION 'wasbs://raw@gaiasa.blob.core.windows.net/year=2017/month=01/type=green/';
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

