// Databricks notebook source
//Storage account configuration
//spark.conf.set(
//  "fs.azure.account.key.demorgsa.blob.core.windows.net",
//  "vp9XbsXu4dHfT5x/BGRKe0k88I/pIy0Zr0XYpg+NUMsEXWzAMWIWjoF5wuafVsckkEmpgzJob3iaD2P9T++Zcw==")


// COMMAND ----------

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

//Schema for data
val customSchema = StructType(Array(
    StructField("region_key", IntegerType, true),
    StructField("region_name", StringType, true),
    StructField("region_comment", StringType, true)))


// COMMAND ----------

//Read contents
val regionsDF = sqlContext.read.format("csv")
  .option("header", "false")
  .schema(customSchema)
  .option("delimiter","|")
  .load("wasbs://sql-dw-demo-container@demorgsa.blob.core.windows.net/region/*")


// COMMAND ----------

regionsDF.count

// COMMAND ----------

//Print schema
regionsDF.printSchema()

// COMMAND ----------

//Print contents
display(regionsDF)

// COMMAND ----------

//Create Hive external table
regionsDF.write.mode(SaveMode.Overwrite).saveAsTable("sales.regions")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from sales.regions where upper(region_name) = 'AFRICA'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC desc formatted sales.regions;

// COMMAND ----------

