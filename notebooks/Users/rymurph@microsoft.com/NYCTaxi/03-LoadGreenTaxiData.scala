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

val greenTripSchema2016H2 = StructType(Array(
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

val greenTripSchema2015H22016H1 = StructType(Array(
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

val greenTripSchema2015H1 = StructType(Array(
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

val greenTripSchemaPre2015 = StructType(Array(
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

val yellowTripSchemaPre2015 = StructType(Array(
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

val yellowTripSchemaColList = """"VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","pickup_longitude","pickup_latitude","RatecodeID","store_and_fwd_flag","dropoff_longitude","dropoff_latitude","PULocationID", "DOLocationID", "payment_type", "fare_amount","extra", "mta_tax" , "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"""

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


//until schema derivation is ready
//var taxiSchema = greenTripSchema2017H1
var taxiSchema : StructType = null

for (j <- 2013 to 2017)
  {
    //Create destination partition - year
    dbutils.fs.mkdirs(destDataDirRoot + "trip_year=" + j) 
    for (i <- 1 to 12) 
    {
      if(tripType == "green")
        if((j == 2013 && i > 7) || (j == 2014))
          taxiSchema = greenTripSchemaPre2015
        else if(j == 2015 && i < 7)
          taxiSchema = greenTripSchema2015H1
        else if((j == 2016 && i < 7) || (j == 2015 && i > 6))
          taxiSchema = greenTripSchema2015H22016H1
        else if(j == 2016 && i > 6)
          taxiSchema = greenTripSchema2016H2
        else if(j == 2017)
          taxiSchema = greenTripSchema2017H1
//       else if(tripType == "yellow")
//         if(j == 2016 && i < 7)
//           taxiSchema = yellowTripSchema20152016H1
//         else if(j == 2016 && i > 6)
//           taxiSchema = yellowTripSchema2016H2
//         else if(j == 2017)
//           taxiSchema = yellowTripSchema2017H1


      /*
      //Determine schema and column listing 
      var taxiDataSchema: org.apache.spark.sql.types.StructType  = null
      var columnList: String = ""
      if(j >= 2009 && j <=2014) //2009-2014 - no schema change
        taxiDataSchema = yellowTripSchema1
      else if(j == 2017) //Schema for 2017
      {
        columnList = yellowTripSchemaColList4And3
        taxiDataSchema = yellowTripSchema4
      }
      else if(j == 2016 && i > 6 ) //Schema for 2016 H2
        taxiDataSchema = yellowTripSchema3
      else if( j == 2015 || (j == 2016 && i < 7)) //Schema for 2015 and 2016 H1
        taxiDataSchema = yellowTripSchema2
      */
      
      //Source file  
      val srcDataFile=srcDataDirRoot + "year=" + j + "/month=" +  "%02d".format(i) + "/type=" + tripType + "/" + tripType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
      println("srcDataFile = " + srcDataFile)
      
      //Calc output files to coalesce to
      val srcSize= fs.getContentSummary(new Path(srcDataFile)).getLength 
      val inputDirSize = fs.getContentSummary(new Path(srcDataFile)).getLength * 3/16
      var outputFileCount = Math.floor(inputDirSize / (minCompactedFileSizeInMB * 1024 * 1024)).toInt
      if (outputFileCount == 0) outputFileCount = 1
      println("inputDirSize = " + inputDirSize)
      println("outputFileCount = " + outputFileCount)

      //Destination file  
      val destDataDir = destDataDirRoot + "/trip_year=" + j + "/trip_month=" + "%02d".format(i)      
      println("destDataDir = " + destDataDir)
      
      //Read file using schema
      val taxiDF = sqlContext.read.format("csv")
                                .option("header", "true")
                                .schema(taxiSchema)
                                .option("delimiter",",")
                                .option("mode", "DROPMALFORMED")
                                .load(srcDataFile).cache()
      
      println("recordCount = " + taxiDF.count)
      
      println("====================================================================================")
      println("Original schema")
      println("====================================================================================")
      taxiDF.printSchema
      
      //Append additional columns and write to common (but unordered) schema
      if(tripType == "green")
        if(j < 2015) {
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
                    .withColumn("improvement_surcharge",lit(""))
          taxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDir)
        }
        else if(j == 2015 && i < 7) {
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
          taxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDir)
        }
        else if((j == 2016 && i < 7) || (j == 2015 && i > 6)) {
          val taxiFormattedDF = taxiDF.withColumn("pulocationid", lit(""))
                    .withColumn("dolocationid", lit(""))
                    .withColumn("junk1",lit(""))
                    .withColumn("junk2",lit(""))
          taxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDir)
        }
        else if(j == 2016 && i > 6) {
          val taxiFormattedDF = taxiDF.withColumn("pickup_longitude", lit(""))
                    .withColumn("pickup_latitude", lit(""))
                    .withColumn("dropoff_longitude", lit(""))
                    .withColumn("dropoff_latitude", lit(""))
                    .withColumn("trip_year",substring(col("lpep_pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("lpep_pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
          taxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDir)
        }
        else if(j == 2017) {
          val taxiFormattedDF = taxiDF.withColumn("pickup_longitude", lit(""))
                    .withColumn("pickup_latitude", lit(""))
                    .withColumn("dropoff_longitude", lit(""))
                    .withColumn("dropoff_latitude", lit(""))
                    .withColumn("trip_year",substring(col("lpep_pickup_datetime"),0, 4))
                    .withColumn("trip_month",substring(col("lpep_pickup_datetime"),6,2))
                    .withColumn("taxi_type",lit(tripType))
                    .withColumn("junk1",lit(""))
                    .withColumn("junk2",lit(""))
          taxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDir)
        }
//       else if(tripType == "yellow")
//         if(j == 2016 && i < 7)
//           taxiSchema = yellowTripSchema20152016H1
//         else if(j == 2016 && i > 6)
//           taxiSchema = yellowTripSchema2016H2
//         else if(j == 2017)
//           taxiSchema = yellowTripSchema2017H1


//      taxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDirRoot + "/trip_year=" + j + "/trip_month=" + "%02d".format(i))

//       if(j == 2017)
//       {
//         //Will use 2017 column order as default - so no need of dataframe.select
//         //We will merely append here
//         val greenTaxiFormattedDF = greenTaxiDF.withColumn("pickup_longitude", lit(""))
//                             .withColumn("pickup_latitude", lit(""))
//                             .withColumn("dropoff_longitude", lit(""))
//                             .withColumn("dropoff_latitude", lit(""))
//                             .withColumn("trip_year",substring(col("lpep_pickup_datetime"),0, 4))
//                             .withColumn("trip_month",substring(col("lpep_pickup_datetime"),6,2))
//                             .withColumn("taxi_type",lit("green"))
//                             .withColumn("junk1",lit(""))
//                             .withColumn("junk2",lit(""))
        
//         println("====================================================================================")
//         println("Final schema")
//         println("====================================================================================")
//         taxiFormattedDF.printSchema
//         println("recordCount = " + taxiFormattedDF.count)

//         greenTaxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDirRoot + "/trip_year=" + j + "/trip_month=" + "%02d".format(i))
//       }
      //TODO-START....................................
      //Add other schemas here; Column list will come into play to ensure columns for all years are ordered
      //For some years, we will need multiple dataframe writes to get to the desired column ordering
      //TODO-END....................................

      // Delete residual files from job operation (_SUCCESS, _start*, _committed*)      
      dbutils.fs.ls(destDataDir).foreach((i: FileInfo) => {
        println("File=" + i.path)
        if (!(i.path contains "parquet"))
        {
            val targetFilename = i.path
            val delStatus = dbutils.fs.rm(targetFilename)
            println("Status of deletion of " + targetFilename + " = " + delStatus)
        }})
      
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


val dataDir=destDataDirRoot + "/trip_year=2016/"
println(dataDir)
val deleteDirStatus = dbutils.fs.rm(dataDir,recurse=true)
println(deleteDirStatus)

// COMMAND ----------

println(destDataDirRoot)

// COMMAND ----------

