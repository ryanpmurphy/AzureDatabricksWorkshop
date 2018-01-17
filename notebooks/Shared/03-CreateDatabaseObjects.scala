// Databricks notebook source
// MAGIC %sql
// MAGIC create database if not exists nyc_db;

// COMMAND ----------

// MAGIC %sql
// MAGIC use nyc_db;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS nyc_db.trips_yellow_raw;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS sales_raw.region (region_key INT, region_name STRING, region_comment STRING) USING parquet
// MAGIC partitioned by year
// MAGIC LOCATION 'wasbs://raw@gaialabsa.blob.core.windows.net/nyc/';