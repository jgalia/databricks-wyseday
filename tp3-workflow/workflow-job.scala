// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).format("csv").load("s3://databricks-wyseday-march-2024/NOAA.csv")

// COMMAND ----------

df.printSchema

// COMMAND ----------

// change with your initials
df.write.format("delta").saveAsTable("INITIAL_noaa_workflow")
