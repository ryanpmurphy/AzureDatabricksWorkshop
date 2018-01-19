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
val yellowTripSchema2017H1 = StructType(Array(
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

//Second half of 2016
val yellowTripSchema2016H2 = StructType(Array(
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

//2015 and 2016 first half of the year
val yellowTripSchema20152016H1 = StructType(Array(
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

//2009 though 2014
val yellowTripSchemaPre2015 = StructType(Array(
    StructField("vendorid", StringType, true),
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
    StructField("payment_type", StringType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("total_amount", DoubleType, true)))

// COMMAND ----------

val fs = FileSystem.get(new Configuration())
val srcDataDirRoot = "/mnt/data/nyctaxi/source/" //Root dir for source data
val destDataDirRoot = "/mnt/data/nyctaxi/raw/" //Root dir for formatted data
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
  for (j <- 2009 to 2017)
  {
    for (i <- 1 to 12) 
    {
      if(j > 2008 && j < 2015)
        taxiSchema = yellowTripSchemaPre2015
      else if(j == 2016 && i > 6)
        taxiSchema = yellowTripSchema2016H2
      else if((j == 2016 && i < 7) || (j == 2015))
        taxiSchema = yellowTripSchema20152016H1
      else if(j == 2017 && i < 7)
        taxiSchema = yellowTripSchema2017H1
    }
  }
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

      if(tripYear > 2008 && tripYear < 2015)
      {
        sourceDF.withColumn("pulocationid", lit(""))
                  .withColumn("dolocationid", lit(""))
                  .withColumn("improvement_surcharge",lit(""))
                  .withColumn("junk1",lit(""))
                  .withColumn("junk2",lit(""))
                  .withColumn("trip_year",substring(col("tpep_pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("tpep_pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("yellow"))

      }
      else if(tripYear == 2016 && tripMonth > 6)
      {
        sourceDF.withColumn("pulocationid", lit(""))
                  .withColumn("dolocationid", lit(""))
                  .withColumn("junk1",lit(""))
                  .withColumn("junk2",lit(""))
                  .withColumn("trip_year",substring(col("tpep_pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("tpep_pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("yellow"))
                  .withColumn("vendorid2", col("vendorid").cast(StringType)).drop("vendorid").withColumnRenamed("vendorid2", "vendorid")

      }
      else if((tripYear == 2016 && tripMonth < 7) || (tripYear == 2015))
      {
        sourceDF.withColumn("pickup_longitude", lit(""))
                  .withColumn("pickup_latitude", lit(""))
                  .withColumn("dropoff_longitude", lit(""))
                  .withColumn("dropoff_latitude", lit(""))
                  .withColumn("trip_year",substring(col("tpep_pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("tpep_pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("yellow"))
                  .withColumn("vendorid2", col("vendorid").cast(StringType)).drop("vendorid").withColumnRenamed("vendorid2", "vendorid")

      }
      else if(tripYear == 2017 && tripMonth < 7)
      {
        sourceDF.withColumn("pickup_longitude", lit(""))
                  .withColumn("pickup_latitude", lit(""))
                  .withColumn("dropoff_longitude", lit(""))
                  .withColumn("dropoff_latitude", lit(""))
                  .withColumn("trip_year",substring(col("tpep_pickup_datetime"),0, 4))
                  .withColumn("trip_month",substring(col("tpep_pickup_datetime"),6,2))
                  .withColumn("taxi_type",lit("yellow"))
                  .withColumn("junk1",lit(""))
                  .withColumn("junk2",lit(""))
                  .withColumn("vendorid2", col("vendorid").cast(StringType)).drop("vendorid").withColumnRenamed("vendorid2", "vendorid")

      }
  else
    sourceDF
}

// COMMAND ----------

//Canonical ordered column list for yellow taxi across years to homogenize schema
//These are actual columns names in the header of source data as is
val canonicalTripSchemaColList = Seq("vendorid","tpep_pickup_datetime","tpep_dropoff_datetime","store_and_fwd_flag","ratecodeid","pulocationid","dolocationid",
                                       "pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count",
                                       "trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount",
                                       "improvement_surcharge","total_amount","trip_year","trip_month","taxi_type"
                                    )

// COMMAND ----------

//Delete any residual data from prior executions for an idempotent run
val dataDir=destDataDirRoot + "/trip_year=2009"
val deleteDirStatus = dbutils.fs.rm(dataDir,recurse=true)
println(deleteDirStatus)

// COMMAND ----------

//Read source data from blob storage
//Homogenize schema across years
//Add columns of interest
//Save as parquet

for (j <- 2017 to 2017)
  {
    //Create destination partition - year
    dbutils.fs.mkdirs(destDataDirRoot + "trip_year=" + j) 
    for (i <- 1 to 12) 
    {
      
      println("Year=" + j + "; Month=" + i)
      
      //Source path  
      val srcDataFile= srcDataDirRoot + "year=" + j + "/month=" +  "%02d".format(i) + "/type=yellow/yellow_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
      println(srcDataFile)
      
      //Destination path  
      val destDataDir = destDataDirRoot + "/trip_year=" + j + "/trip_month=" + "%02d".format(i) + "/taxi_type=yellow"

      //Source schema
      val taxiSchema = getTaxiSchema(j,i)

      //Read source data
      val taxiDF = sqlContext.read.format("csv")
                      .option("header", "true")
                      .schema(taxiSchema)
                      .option("delimiter",",")
                      .option("mode", "DROPMALFORMED")
                      .load(srcDataFile).cache()

      //Add additional columns to homogenize schema across years
      val taxiFormattedDF = getSchemaHomogenizedDataframe(taxiDF, j, i)
      
      //Order all columns to align with the canonical schema for yellow taxi
      val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)

      //Write parquet output, calling function to calculate number of partition files
      taxiCanonicalDF.coalesce(calcOutputFileCount(srcDataFile)).write.parquet(destDataDir)

      //Delete residual files from job operation (_SUCCESS, _start*, _committed*)
      dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
    }
  }

// COMMAND ----------

val taxiDF = sqlContext.read.format("csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .option("delimiter",",")
                      .option("mode", "DROPMALFORMED")
                      .load("/mnt/data/nyctaxi/source/year=2009/month=01/type=yellow/yellow_tripdata_2009-01.csv").cache()
  //.(yellowTripSchemaPre2015)


taxiDF.printSchema
taxiDF.head()

// COMMAND ----------

dbutils.fs.head("/mnt/data/nyctaxi/source/year=2009/month=01/type=yellow/yellow_tripdata_2009-01.csv")

// COMMAND ----------

//Clean up for rerun
for (j <- 2017 to 2017)
  {
    for (i <- 1 to 6) 
    {
      
      println("Year=" + j + "; Month=" + i)
      println("====================================================================================")
      println("...1.  Deleting files")
      
      val dataDir=destDataDirRoot + "trip_year=" + j + "/trip_month=" +  "%02d".format(i) + "/"
      println("dataDir=" + dataDir)

      dbutils.fs.ls(dataDir).foreach((i: FileInfo) => {
        val filename = i.path  
        val delStatus = dbutils.fs.rm(filename)
        println("........Status of deletion of " + filename + " = " + delStatus)
      })
      
      println("...2.  Deleting directory")
      dbutils.fs.rm(dataDir)
      println("====================================================================================")
    }
}

// COMMAND ----------



// COMMAND ----------

