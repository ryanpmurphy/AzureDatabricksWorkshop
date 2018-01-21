// Databricks notebook source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

// COMMAND ----------

//Schema for data based on year and month

//2017
val greenTripSchema2017H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

//Second half of 2016
val greenTripSchema2016H2 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

//2015 second half of the year and 2016 first half of the year
val greenTripSchema2015H22016H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

//2015 first half of the year
val greenTripSchema2015H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

//August 2013 through 2014
val greenTripSchemaPre2015 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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
    StructField("total_amount", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("trip_type", IntegerType, true),
    StructField("junk1", StringType, true),
    StructField("junk2", StringType, true)))


// COMMAND ----------

val fs = FileSystem.get(new Configuration())
val srcDataDirRoot = "/mnt/data/nyctaxi/source/" //Root dir for source data
val destDataDirRoot = "/mnt/data/nyctaxi/raw/green-taxi" //Root dir for formatted data
val targetedFileSizeMB = 64 //Dont want very small files - we want to keep each part file at least 64 MB
val parquetSpaceSaving = 0.19 //We found a saving in space of 19% with Parquet

// COMMAND ----------

//1) Function to calculate # of output files
//Purpose: Without this function, Spark will create numerous small files baed on various criteria
//         This is to control the output file size to keep it at least 64 MB in size
//Input:  Source file  (CSV) path
//Output: Targeted file count for output
//Sample call: println(calcOutputFileCount(srcDataDirRoot + "year=2017/month=06/type=yellow/yellow_tripdata_2017-06.csv"))

val calcOutputFileCount = (srcDataFile: String) => 
{
  val estFileCount: Int = Math.floor((fs.getContentSummary(new Path(srcDataFile)).getLength * parquetSpaceSaving) / (targetedFileSizeMB * 1024 * 1024)).toInt
  
  if(estFileCount == 0) 1 else estFileCount
}

// COMMAND ----------

//2) Function to determine schema for a given year and month
//Input:  Year and month
//Output: StructType for applicable schema 
//Sample call: println(getSchemaStruct(2009,1))

val getTaxiSchema  = (tripYear: Int, tripMonth: Int) => {
  var taxiSchema : StructType = null

      if((tripYear == 2013 && tripMonth > 7) || tripYear == 2014)
        taxiSchema = greenTripSchemaPre2015
      else if(tripYear == 2015 && tripMonth < 7)
        taxiSchema = greenTripSchema2015H1
      else if((tripYear == 2015 && tripMonth > 6) || (tripYear == 2016 && tripMonth < 7))
        taxiSchema = greenTripSchema2015H22016H1
      else if(tripYear == 2016 && tripMonth > 6)
        taxiSchema = greenTripSchema2016H2
      else if(tripYear == 2017 && tripMonth < 7)
        taxiSchema = greenTripSchema2017H1
  
  taxiSchema
}

// COMMAND ----------

//3) Function to add columns to dataframe as required to homogenize schema
//Input:  Dataframe, year and month
//Output: Dataframe with homogenized schema 
//Sample call: println(getSchemaHomogenizedDataframe(DF,2014,6))

import org.apache.spark.sql.DataFrame

def getSchemaHomogenizedDataframe(sourceDF: org.apache.spark.sql.DataFrame,
                                  tripYear: Int, 
                                  tripMonth: Int) 
                                  : org.apache.spark.sql.DataFrame =
{  

      if((tripYear == 2013 && tripMonth > 7) || tripYear == 2014)
      {
        sourceDF.withColumn("pulocationid", lit(0).cast(IntegerType))
                  .withColumn("dolocationid", lit(0).cast(IntegerType))
                  .withColumn("improvement_surcharge",lit(0).cast(DoubleType))
                  .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("green"))
                  .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                          .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                  .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                          .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                  .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                          .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                  .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                          .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
      }
      else if(tripYear == 2015 && tripMonth < 7)
      {
        sourceDF.withColumn("pulocationid", lit(0).cast(IntegerType))
                  .withColumn("dolocationid", lit(0).cast(IntegerType))
                  .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("green"))
                  .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                          .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                  .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                          .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                  .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                          .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                  .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                          .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
      }
      else if((tripYear == 2015 && tripMonth > 6) || (tripYear == 2016 && tripMonth < 7))
      {
        sourceDF.withColumn("pulocationid", lit(0).cast(IntegerType))
                  .withColumn("dolocationid", lit(0).cast(IntegerType))
                  .withColumn("junk1",lit(""))
                  .withColumn("junk2",lit(""))
                  .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("green"))
                  .withColumn("temp_pickup_longitude", col("pickup_longitude").cast(StringType))
                                          .drop("pickup_longitude").withColumnRenamed("temp_pickup_longitude", "pickup_longitude")
                  .withColumn("temp_dropoff_longitude", col("dropoff_longitude").cast(StringType))
                                          .drop("dropoff_longitude").withColumnRenamed("temp_dropoff_longitude", "dropoff_longitude")
                  .withColumn("temp_pickup_latitude", col("pickup_latitude").cast(StringType))
                                          .drop("pickup_latitude").withColumnRenamed("temp_pickup_latitude", "pickup_latitude")
                  .withColumn("temp_dropoff_latitude", col("dropoff_latitude").cast(StringType))
                                          .drop("dropoff_latitude").withColumnRenamed("temp_dropoff_latitude", "dropoff_latitude")
      }
      else if(tripYear == 2016 && tripMonth > 6)
      {
        sourceDF.withColumn("pickup_longitude", lit(""))
                  .withColumn("pickup_latitude", lit(""))
                  .withColumn("dropoff_longitude", lit(""))
                  .withColumn("dropoff_latitude", lit(""))
                  .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("green"))
      }
      else if(tripYear == 2017 && tripMonth < 7)
      {
        sourceDF.withColumn("pickup_longitude", lit(""))
                  .withColumn("pickup_latitude", lit(""))
                  .withColumn("dropoff_longitude", lit(""))
                  .withColumn("dropoff_latitude", lit(""))
                  .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("green"))
                  .withColumn("junk1",lit(""))
                  .withColumn("junk2",lit(""))
      }
  else
    sourceDF
}

// COMMAND ----------

//Canonical ordered column list for green taxi across years to homogenize schema
//These are actual columns names in the header of source data as is
val canonicalTripSchemaColList = Seq("taxi_type","vendorid","pickup_datetime","dropoff_datetime","store_and_fwd_flag","ratecodeid","pulocationid","dolocationid","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count","trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","ehail_fee","improvement_surcharge","total_amount","payment_type","trip_type","trip_year","trip_month")

// COMMAND ----------

//Delete any residual data from prior executions for an idempotent run
val dataDir=destDataDirRoot 
val deleteDirStatus = dbutils.fs.rm(dataDir,recurse=true)
println(deleteDirStatus)

// COMMAND ----------

//Read source data from blob storage
//Homogenize schema across years
//Add columns of interest
//Save as parquet

import spark.implicits._
import spark.sql

for (j <- 2014 to 2017)
  {
    val monthsCount = if (j==2017) 6 else 12 
    for (i <- 1 to monthsCount) 
    {
      
      //Source path  
      val srcDataFile= srcDataDirRoot + "year=" + j + "/month=" +  "%02d".format(i) + "/type=green/green_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
      println("Year=" + j + "; Month=" + i)
      println(srcDataFile)


      //Destination path  
      val destDataDir = destDataDirRoot + "/trip_year=" + j + "/trip_month=" + "%02d".format(i) + "/"
      
      //Source schema
      val taxiSchema = getTaxiSchema(j,i)

      //Read source data
      val taxiDF = sqlContext.read.format("csv")
                      .option("header", "true")
                      .schema(taxiSchema)
                      .option("delimiter",",")
                      .load(srcDataFile).cache()

      //Add additional columns to homogenize schema across years
      val taxiFormattedDF = getSchemaHomogenizedDataframe(taxiDF, j, i)

      //Order all columns to align with the canonical schema for green taxi
      val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)

      //To make Hive Parquet format compatible with Spark Parquet format
      spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

      //Write parquet output, calling function to calculate number of partition files
      taxiCanonicalDF.coalesce(calcOutputFileCount(srcDataFile)).write.parquet(destDataDir)

      //Delete residual files from job operation (_SUCCESS, _start*, _committed*)
      dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))

      
      //Add partition for year and month
      sql("ALTER TABLE nyc_db.trips_green_raw_prq ADD IF NOT EXISTS PARTITION (trip_year=" + j + ",trip_month=" + "%02d".format(i) + ") LOCATION '" + destDataDir.dropRight(1) + "'")
    
      //Refresh table
      sql("REFRESH TABLE nyc_db.trips_green_raw_prq")
    }
  }
sql("ANALYZE TABLE nyc_db.trips_green_raw_prq COMPUTE STATISTICS")


// COMMAND ----------

//Check if file exists - Test
//val exists = fs.exists(new Path("/mnt/data/nyctaxi/source/year=2016/month=01/type=green/"))


// COMMAND ----------

//dbutils.fs.head("/mnt/data/nyctaxi/source/year=2016/month=01/type=green/green_tripdata_2016-01.csv")

// COMMAND ----------

