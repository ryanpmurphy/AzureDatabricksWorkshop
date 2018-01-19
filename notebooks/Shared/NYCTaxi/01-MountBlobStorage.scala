// Databricks notebook source
//Test

// COMMAND ----------

// Mount Azure Blob Storage (supported in Databricks runtime 4 and >)
// Source dataset
dbutils.fs.mount(
  source = "wasbs://nyc@gaiasa.blob.core.windows.net/",
  mountPoint = "/mnt/data/nyctaxi/source/",
  extraConfigs = Map("fs.azure.account.key.gaiasa.blob.core.windows.net" -> "bST3bnOVxbdEdKuevIp2JlAjUc4mu81CAxBcmU+w7JfGzQ2SZG8B4sQwnxrFnRoH18KzYlDAvgdogK7FN0wpYQ=="))

// COMMAND ----------

// Mount Azure Blob Storage (supported in Databricks runtime 4 and >)
// Raw dataset
dbutils.fs.mount(
  source = "wasbs://raw@gaiasa.blob.core.windows.net/",
  mountPoint = "/mnt/data/nyctaxi/raw/",
  extraConfigs = Map("fs.azure.account.key.gaiasa.blob.core.windows.net" -> "bST3bnOVxbdEdKuevIp2JlAjUc4mu81CAxBcmU+w7JfGzQ2SZG8B4sQwnxrFnRoH18KzYlDAvgdogK7FN0wpYQ=="))

// COMMAND ----------

//Refresh mounts
dbutils.fs.refreshMounts()