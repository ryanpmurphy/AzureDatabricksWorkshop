// Databricks notebook source
displayHTML("<font name='arial'><h1>Download NYC taxi data</h1><br>1) Configure storage destination<br>2) Download using wget to local, and upload to blob storage<br>3) Delete from local<br>Note: Direct download to blob mount is not supported<br><br><br>                  </font>")




// COMMAND ----------

//imports
import sys.process._
import scala.sys.process.ProcessLogger

// COMMAND ----------

//================================================================================
// 1) STORAGE CONFIGURATION
// This is not required if storage conf is set at cluster level
//================================================================================
spark.conf.set(
  "fs.azure.account.key.gaialabsa.blob.core.windows.net",
  "bXE4JN3iQ32ANf6xtpwRxZqYZk8TlAXCXacASB84jg91TsFiz5sLEzy2v5+ThiBgfrf6qVkkbxkxGPahbEoF7Q==")


// COMMAND ----------

//================================================================================
// 2. DOWNLOAD LOOKUP DATA
//================================================================================
"wget -P /tmp https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv" !!

val localPath="file:/tmp/taxi+_zone_lookup.csv"
val wasbPath="wasbs://source@gaialabsa.blob.core.windows.net/nyc/refdata"

dbutils.fs.mkdirs(wasbPath)
dbutils.fs.cp(localPath, wasbPath)
dbutils.fs.rm(localPath)
display(dbutils.fs.ls(wasbPath))

// COMMAND ----------

//================================================================================
// 2. DOWNLOAD 2017 TRANSACTIONAL DATA
//================================================================================
val cabTypes = Seq("yellow", "green", "fhv")
for (cabType <- cabTypes) {
  for (i <- 1 to 6) 
  {
    val fileName = cabType + "_tripdata_2017-0" + i + ".csv"
    val wasbPath="wasbs://source@gaialabsa.blob.core.windows.net/nyc/year=2017/month=0" + i + "/type=" + cabType + "/"
    val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
    println(wgetToExec)
  
    wgetToExec !!
  
    val localPath="file:/tmp/" + fileName
    dbutils.fs.mkdirs(wasbPath)
    dbutils.fs.cp(localPath, wasbPath)
    dbutils.fs.rm(localPath)
    display(dbutils.fs.ls(wasbPath))
  }
}

// COMMAND ----------

//================================================================================
// 3. DOWNLOAD 2014-16 TRANSACTIONAL DATA
//================================================================================
val cabTypes = Seq("yellow", "green", "fhv")

for (cabType <- cabTypes) {
  for (j <- 2014 to 2016)
  {
    for (i <- 1 to 12) 
    {
      val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
      println(fileName)
      val wasbPath="wasbs://source@gaialabsa.blob.core.windows.net/nyc/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
      val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
      println(wgetToExec)

      wgetToExec !!

      val localPath="file:/tmp/" + fileName
      dbutils.fs.mkdirs(wasbPath)
      dbutils.fs.cp(localPath, wasbPath)
      dbutils.fs.rm(localPath)
      display(dbutils.fs.ls(wasbPath))
    } 
  }
}

// COMMAND ----------

//================================================================================
// 4. DOWNLOAD 2013 TRANSACTIONAL DATA
//================================================================================

val j = "2013"
val cabType = "yellow"
for (i <- 1 to 7) 
{
  val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
  println(fileName)
  val wasbPath="wasbs://source@gaialabsa.blob.core.windows.net/nyc/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
  val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
  println(wgetToExec)

  wgetToExec !!

  val localPath="file:/tmp/" + fileName
  dbutils.fs.mkdirs(wasbPath)
  dbutils.fs.cp(localPath, wasbPath)
  dbutils.fs.rm(localPath)
  display(dbutils.fs.ls(wasbPath))
} 

val cabTypes = Seq("yellow", "green")
for (cabType <- cabTypes) {
  for (i <- 8 to 12) 
  {
    val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
    println(fileName)
    val wasbPath="wasbs://source@gaialabsa.blob.core.windows.net/nyc/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
    val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
    println(wgetToExec)

    wgetToExec !!

    val localPath="file:/tmp/" + fileName
    dbutils.fs.mkdirs(wasbPath)
    dbutils.fs.cp(localPath, wasbPath)
    dbutils.fs.rm(localPath)
    display(dbutils.fs.ls(wasbPath))
  } 
}

// COMMAND ----------

//================================================================================
// 5. DOWNLOAD 2009-12 TRANSACTIONAL DATA
//================================================================================

val cabType = "yellow"
for (j <- 2009 to 2012)
{
  for (i <- 1 to 12) 
  {
    val fileName = cabType + "_tripdata_" + j + "-" + "%02d".format(i) + ".csv"
    println(fileName)
    val wasbPath="wasbs://source@gaialabsa.blob.core.windows.net/nyc/year=" + j + "/month=" +  "%02d".format(i) + "/type=" + cabType + "/"
    val wgetToExec = "wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/" + fileName
    println(wgetToExec)

    wgetToExec !!

    val localPath="file:/tmp/" + fileName
    dbutils.fs.mkdirs(wasbPath)
    dbutils.fs.cp(localPath, wasbPath)
    dbutils.fs.rm(localPath)
    display(dbutils.fs.ls(wasbPath))
  } 
}

// COMMAND ----------

// MAGIC %fs ls "/mnt/data/nycdata/year=2017/month=06/type=yellow"

// COMMAND ----------

// MAGIC %fs head "dbfs:/mnt/data/nycdata/year=2017/month=06/type=yellow/yellow_tripdata_2017-06.csv"

// COMMAND ----------

//Links:
//https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html