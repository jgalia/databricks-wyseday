// Databricks notebook source
// MAGIC %md
// MAGIC # Import 

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.row_number
import com.github.nscala_time.time.Imports._
import org.joda.time.Days

// COMMAND ----------

// MAGIC %md
// MAGIC # via scala

// COMMAND ----------

// MAGIC %md
// MAGIC a markdown title allows you to create a part in the notebook that can be folded (-/+ symbol) to the left of the title insert

// COMMAND ----------

// MAGIC %md
// MAGIC help documentation : https://docs.databricks.com/en/delta/tutorial.html

// COMMAND ----------

// MAGIC %md
// MAGIC read data from s3 : s3://databricks-wyseday-march-2024/NOAA.csv
// MAGIC * format csv
// MAGIC * first row : columns names
// MAGIC * infer schema automatically

// COMMAND ----------

// complete the code
val noaa = 

// COMMAND ----------

// here you should be able to display the first 10 lines
display()

// COMMAND ----------

// MAGIC %md
// MAGIC ## write a delta table

// COMMAND ----------

// MAGIC %md
// MAGIC write the table in delta format to the path s3://databricks-wyseday-march-2024/**VOS-INITIALES**/delta_NOAA
// MAGIC
// MAGIC example path: s3://databricks-wyseday-march-2024/JG/delta_NOAA
// MAGIC
// MAGIC settings:
// MAGIC * partition : year
// MAGIC * format : delta
// MAGIC * option : "partitionOverwriteMode" -> "dynamic"

// COMMAND ----------

noaa
  .withColumn() //?
  .write
  .mode("overwrite")
  . // ?
  .options() //?
  . //?
  . //

// COMMAND ----------

// MAGIC %md
// MAGIC ## optimize

// COMMAND ----------

// MAGIC %md
// MAGIC documentation : https://docs.databricks.com/en/delta/optimize.html#syntax-examples

// COMMAND ----------

// via spark conf :
spark. ?
// then rewrite table 

// COMMAND ----------

// via spark action
import ?
val deltaTable = ?


// COMMAND ----------

// MAGIC %md
// MAGIC ## vacuum

// COMMAND ----------

// MAGIC %md
// MAGIC documentation : https://docs.databricks.com/en/delta/vacuum.html#example-syntax-for-vacuum

// COMMAND ----------

// MAGIC %md
// MAGIC here an example of a bug on vacuum

// COMMAND ----------

// MAGIC %sql
// MAGIC  ? 0 HOURS

// COMMAND ----------

// fix the bug with spark conf
spark.conf.set(?)

// COMMAND ----------

// make vacuum on your table s3://databricks-wyseday-march-2024/INITIALES/delta_NOAA
%sql
? 0 HOURS

// COMMAND ----------

// MAGIC %md
// MAGIC # via sql

// COMMAND ----------

// MAGIC %md
// MAGIC ### write in the catalog

// COMMAND ----------

// Register df into the catalog as: initiale_noaa  
// ex: JG_noaa
noaa. ?

// COMMAND ----------

// MAGIC %md
// MAGIC ### explorer les schemas

// COMMAND ----------

// MAGIC %sql
// MAGIC -- print databases

// COMMAND ----------

// MAGIC %sql
// MAGIC -- set a database

// COMMAND ----------

// MAGIC %sql
// MAGIC -- print tables

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read data from the metastore

// COMMAND ----------

// MAGIC %sql
// MAGIC -- read the table you just created

// COMMAND ----------

// other way
val sqlDf = spark.sql("SELECT * FROM ?")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read data from s3

// COMMAND ----------

// MAGIC %md
// MAGIC Read data from s3
// MAGIC
// MAGIC documentation : https://docs.databricks.com/en/delta/tutorial.html#read-a-table

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM ? limit 10;
