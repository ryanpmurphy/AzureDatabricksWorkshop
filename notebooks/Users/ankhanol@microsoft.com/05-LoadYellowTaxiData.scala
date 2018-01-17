// Databricks notebook source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import com.databricks.backend.daemon.dbutils.FileInfo
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration

// COMMAND ----------

//Schema for year 2017 H1
val yellowTripSchema4 = StructType(Array(
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


//Schema for year 2016 H2
val yellowTripSchema3 = StructType(Array(
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
    StructField("total_amount", DoubleType, true),
    StructField("junk1", StringType, true),
    StructField("junk2", StringType, true)))

//Schema for year 2015 and 2016 H1
val yellowTripSchema2 = StructType(Array(
    StructField("vendorid", IntegerType, true),
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("pickup_longitude",DoubleType, true),
    StructField("pickup_latitude",DoubleType, true),
    StructField("ratecodeid", IntegerType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("dropoff_longitude",DoubleType, true),
    StructField("dropoff_latitude",DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", IntegerType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true)))

//Schema for year 2009-2014
val yellowTripSchema1 = StructType(Array(
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
    StructField("total_amount", DoubleType, true)))

val yellowTripSchemaColList4And3 = """"VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","RatecodeID","store_and_fwd_flag","PULocationID", "DOLocationID", "payment_type", "fare_amount","extra", "mta_tax" , "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"""

// COMMAND ----------

// Variables and other parameters

// Source filename format = yellow_tripdata_YYYY-MM.csv
val srcFileNamePrefix = "yellow_tripdata_" 
val srcFileNameSuffix = ".csv"

// Source file directory
val srcDataDirRoot = "/mnt/data/nycdata/source"
val destDataDirRoot = "/mnt/data/nycdata/raw/nyc/"

// Others
val fs = FileSystem.get(new Configuration())
val minCompactedFileSizeInMB = 64

// COMMAND ----------

for (j <- 2017 to 2017)
  {
    //Create destination partition - year
    dbutils.fs.mkdirs(destDataDirRoot + "trip_year=" + j) 
    for (i <- 1 to 6) 
    {
      
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
      val srcDataFile=srcDataDirRoot + "/year=" + j + "/month=" +  "%02d".format(i) + "/type=yellow/" + "yellow_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
      println("srcDataFile = " + srcDataFile)
      
      //Calc output files to coalesce to
      val rawSize= fs.getContentSummary(new Path(srcDataFile)).getLength 
      val inputDirSize = fs.getContentSummary(new Path(srcDataFile)).getLength * 3/16
      var outputFileCount = Math.floor(inputDirSize / (minCompactedFileSizeInMB * 1024 * 1024)).toInt
      if (outputFileCount == 0) outputFileCount = 1
      println("inputDirSize = " + inputDirSize)
      println("outputFileCount = " + outputFileCount)
      
      //Read file using schema
      val yellowTaxiDF = sqlContext.read.format("csv")
                                .option("header", "true")
                                .option("inferSchema","true")
                                .option("delimiter",",")
                                .option("mode", "DROPMALFORMED")
                                .load(srcDataFile).cache()
      
      println("recordCount = " + yellowTaxiDF.count)
      
      println("====================================================================================")
      println("Original schema")
      println("====================================================================================")
      yellowTaxiDF.printSchema
      
      //Append additional columns
      if(j == 2017)
      {
        //Will use 2017 column order as default - so no need of dataframe.select
        //We will merely append here
        val yellowTaxiFormattedDF = yellowTaxiDF.withColumn("pickup_longitude", lit(""))
                            .withColumn("pickup_latitude", lit(""))
                            .withColumn("dropoff_longitude", lit(""))
                            .withColumn("dropoff_latitude", lit(""))
                            .withColumn("trip_year",substring(col("tpep_pickup_datetime"),0, 4))
                            .withColumn("trip_month",substring(col("tpep_pickup_datetime"),6,2))
                            .withColumn("taxi_type",lit("yellow"))
        
        println("====================================================================================")
        println("Final schema")
        println("====================================================================================")
        yellowTaxiFormattedDF.printSchema
        println("recordCount = " + yellowTaxiFormattedDF.count)

        yellowTaxiFormattedDF.coalesce(outputFileCount).write.parquet(destDataDirRoot + "trip_year=" + j + "/trip_month=" + "%02d".format(i))
      }
      //TODO-START....................................
      //Add other schemas here; Column list will come into play to ensure columns for all years are ordered
      //For some years, we will need multiple dataframe writes to get to the desired column ordering
      //TODO-END....................................
      
      // Delete residual files from job operation (_SUCCESS, _start*, _committed*)      
      dbutils.fs.ls(destDataDirRoot + "trip_year=" + j + "/trip_month=" + "%02d".format(i)).foreach((i: FileInfo) => {
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

println("Now deleting residual files in the root dir")
dbutils.fs.ls(destDataDirRoot).foreach((i: FileInfo) => {
        val filename = i.path  
        val delStatus = dbutils.fs.rm(filename)
        println("........Status of deletion of " + filename + " = " + delStatus)
      })

// COMMAND ----------

//Get file count
val srcDataFile=srcDataDirRoot + "/year=2017/month=01/type=yellow/yellow_tripdata_2017-01.csv"
println("srcDataFile = " + srcDataFile)
      
//Calc output files to coalesce to
val rawSize= fs.getContentSummary(new Path(srcDataFile)).getLength 
println("rawSize = " + rawSize)

val inputDirSize = fs.getContentSummary(new Path(srcDataFile)).getLength * 3/16
var outputFileCount = Math.floor(inputDirSize / (minCompactedFileSizeInMB * 1024 * 1024)).toInt
if (outputFileCount == 0) outputFileCount = 1
println("inputDirSize = " + inputDirSize)
println("outputFileCount = " + outputFileCount)

// COMMAND ----------

