// Databricks notebook source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration

// COMMAND ----------

//Schema for data
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
    //StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("trip_type", IntegerType, true),
    StructField("junk1", StringType, true),
    StructField("junk2", StringType, true)))

val yellowTripSchema2017H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

val yellowTripSchema2016H2 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

val yellowTripSchema20152016H1 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

val yellowTripSchemaPre2015 = StructType(Array(
    StructField("vendorid", StringType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
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

// val yellowTripSchemaColList = """"VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","pickup_longitude","pickup_latitude","RatecodeID","store_and_fwd_flag","dropoff_longitude","dropoff_latitude","PULocationID", "DOLocationID", "payment_type", "fare_amount","extra", "mta_tax" , "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"""

//missing partition columns: trip_year, trip_month, taxi_type
// val canonicalTripSchemaColList = """"VendorID","pickup_datetime","dropoff_datetime","passenger_count","trip_distance","pickup_longitude","pickup_latitude","RatecodeID","store_and_fwd_flag","dropoff_longitude","dropoff_latitude","PULocationID", "DOLocationID", "payment_type", "fare_amount","extra", "mta_tax" , "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"""



// COMMAND ----------

// Variables and other parameters

// Taxi trip type, yellow or green
val tripType = "green"

// Source filename format = yellow_tripdata_YYYY-MM.csv
val srcFileNamePrefix = tripType + "_tripdata_" 
val srcFileNameSuffix = ".csv"

// Source file directory
val srcDataDirRoot = "/mnt/data/nyctaxi/source/"
val destDataDirRoot = "/mnt/data/nyctaxi/raw/"

// Others
val fs = FileSystem.get(new Configuration())
val minCompactedFileSizeInMB = 64


// COMMAND ----------

//Function to calculate # of partition files
//accepts file path and target partition file size
val outputFileCount = (srcDataFile: String, minCompactedFileSizeInMB: Int) => {
  val estFileCount = Math.floor((fs.getContentSummary(new Path(srcDataFile)).getLength * 3/16) / (minCompactedFileSizeInMB * 1024 * 1024)).toInt
  if(estFileCount == 0) 1 else estFileCount
}

// COMMAND ----------

//Set canonical column list
val canonicalTripSchemaColList = Seq("vendorid","pickup_datetime","dropoff_datetime","store_and_fwd_flag","ratecodeid","pulocationid","dolocationid",
                                       "pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count",
                                       "trip_distance","fare_amount","extra","mta_tax","tip_amount","tolls_amount","ehail_fee",
                                       "improvement_surcharge","total_amount","payment_type","trip_year","trip_month","taxi_type")

// COMMAND ----------

//Load Source data to Raw
///////////////////////////////

var taxiSchema : StructType = null

for (j <- 2015 to 2017)
  {
    //Create destination partition - year
    dbutils.fs.mkdirs(destDataDirRoot + "trip_year=" + j) 
    for (i <- 1 to 12) 
    {
      //Source file  
      val srcDataFile=srcDataDirRoot + "year=" + j + "/month=" +  "%02d".format(i) + "/type=" + tripType + "/" + tripType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
      println("srcDataFile = " + srcDataFile)
      //Destination directory  
      val destDataDir = destDataDirRoot + "/trip_year=" + j + "/trip_month=" + "%02d".format(i) + "/trip_type=" + tripType
      println("destDataDir = " + destDataDir)

      //GREEN TRIPS
      if(tripType == "green"){
        if((j == 2013 && i > 7) || (j == 2014)){
          //Set schema
          taxiSchema = greenTripSchemaPre2015
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          //Add columns
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
                    .withColumn("improvement_surcharge",lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
        else if(j == 2015 && i < 7){
          //Set schema
          taxiSchema = greenTripSchema2015H1
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          //Add columns
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
        else if((j == 2016 && i < 7) || (j == 2015 && i > 6)){
          //Set schema
          taxiSchema = greenTripSchema2015H22016H1
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          // Add columns
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
                    .withColumn("junk1",lit(""))
                    .withColumn("junk2",lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
        else if(j == 2016 && i > 6){
          //Set schema
          taxiSchema = greenTripSchema2016H2
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          // Add columns
          val taxiFormattedDF = taxiDF.withColumn("pickup_longitude", lit(""))
                    .withColumn("pickup_latitude", lit(""))
                    .withColumn("dropoff_longitude", lit(""))
                    .withColumn("dropoff_latitude", lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
        else if(j == 2017 && i < 7){
          //Set schema
          taxiSchema = greenTripSchema2017H1
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          // Add columns
          val taxiFormattedDF = taxiDF.withColumn("pickup_longitude", lit(""))
                    .withColumn("pickup_latitude", lit(""))
                    .withColumn("dropoff_longitude", lit(""))
                    .withColumn("dropoff_latitude", lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
                    .withColumn("junk1",lit(""))
                    .withColumn("junk2",lit(""))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
      }

      //YELLOW TRIPS
      else if(tripType == "yellow"){
        if(j > 2008 && j < 2015){
          //Set schema
          taxiSchema = yellowTripSchemaPre2015
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          //Add columns
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
                    .withColumn("improvement_surcharge",lit(""))
                    .withColumn("junk1",lit(""))
                    .withColumn("junk2",lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
                    .withColumn("ehail_fee",lit(""))
                    .withColumn("trip_type",lit(""))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
        else if((j == 2016 && i < 7) || (j == 2015)){
          //Set schema
          taxiSchema = yellowTripSchema20152016H1
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          //Add columns
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
                    .withColumn("junk1",lit(""))
                    .withColumn("junk2",lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
                    .withColumn("ehail_fee",lit(""))
                    .withColumn("trip_type",lit(""))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
        else if(j == 2016 && i > 6){
          //Set schema
          taxiSchema = yellowTripSchema2016H2
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          //Add columns
          val taxiFormattedDF = taxiDF.withColumn("pickup_longitude", lit(""))
                    .withColumn("pickup_latitude", lit(""))
                    .withColumn("dropoff_longitude", lit(""))
                    .withColumn("dropoff_latitude", lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
                    .withColumn("ehail_fee",lit(""))
                    .withColumn("trip_type",lit(""))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
        else if(j == 2017 && i < 7){
          //Set schema
          taxiSchema = yellowTripSchema2017H1
          //Read file using schema
          val taxiDF = sqlContext.read.format("csv").option("header", "true").schema(taxiSchema).option("delimiter",",").option("mode", "DROPMALFORMED").load(srcDataFile).cache()
          //Add columns
          val taxiFormattedDF = taxiDF.withColumn("pickup_longitude", lit(""))
                    .withColumn("pickup_latitude", lit(""))
                    .withColumn("dropoff_longitude", lit(""))
                    .withColumn("dropoff_latitude", lit(""))
                    .withColumn("trip_year",substring(col("pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
                    .withColumn("junk1",lit(""))
                    .withColumn("junk2",lit(""))
                    .withColumn("ehail_fee",lit(""))
                    .withColumn("trip_type",lit(""))
           val taxiCanonicalDF = taxiFormattedDF.select(canonicalTripSchemaColList.map(c => col(c)): _*)
          //Write parquet output, calling function to calculate number of partition files
          taxiCanonicalDF.coalesce(outputFileCount(srcDataFile, minCompactedFileSizeInMB)).write.parquet(destDataDir)
          // Delete residual files from job operation (_SUCCESS, _start*, _committed*)
          dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
        }
      }
    } 
  }

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


val dataDir=destDataDirRoot + "/trip_year=2016"

val deleteDirStatus = dbutils.fs.rm(dataDir,recurse=true)
println(deleteDirStatus)

// COMMAND ----------

println(destDataDirRoot)

// COMMAND ----------

val trips = spark.read.parquet("/mnt/data/nyctaxi/raw/trip_year=2013/trip_month=08/trip_type=green")
display(trips)

// COMMAND ----------

