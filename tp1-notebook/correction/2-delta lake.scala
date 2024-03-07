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
// MAGIC un titre en markdown permet de créer une partie dans le notebook qui peut se replier (symbole -/+) à gauche de l'encart du titre

// COMMAND ----------

// MAGIC %md
// MAGIC documentation pour le code : https://docs.databricks.com/en/delta/tutorial.html

// COMMAND ----------

// MAGIC %md
// MAGIC lire les données du fichier s3 : s3://databricks-wyseday-march-2024/NOAA.csv
// MAGIC * format csv
// MAGIC * 1ière ligne : nom des colonnes
// MAGIC * inférer schema automatiquement

// COMMAND ----------

// complete the code
val noaa = spark.read.format("csv")
.options(Map("header"->"true", "inferSchema"-> "true"))
.load("s3://databricks-wyseday-march-2024/NOAA.csv")

// COMMAND ----------

// ici vous devez pouvoir afficher les 10 premières lignes
display(noaa.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ## write a delta table

// COMMAND ----------

// MAGIC %md
// MAGIC écrire la table en format delta sur le path s3://databricks-wyseday-march-2024/**VOS-INITIALES**/delta_NOAA 
// MAGIC
// MAGIC exemple de path: s3://databricks-wyseday-march-2024/JG/delta_NOAA
// MAGIC
// MAGIC paramètres:
// MAGIC * partition : year
// MAGIC * format : delta
// MAGIC * option : "partitionOverwriteMode" -> "dynamic"

// COMMAND ----------

noaa
  .withColumn("year", year(col("date")))
  .write
  .mode("overwrite")
  .format("delta")
  .options(Map("partitionOverwriteMode" -> "dynamic"))
  .partitionBy("year")
  .save("s3://databricks-wyseday-march-2024/JG/delta_NOAA")

// COMMAND ----------

// MAGIC %md
// MAGIC ## optimize

// COMMAND ----------

// MAGIC %md
// MAGIC documentation : https://docs.databricks.com/en/delta/optimize.html#syntax-examples

// COMMAND ----------

// via spark conf :
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
// then rewrite table 

// COMMAND ----------

// via spark action
import io.delta.tables._
val deltaTable = DeltaTable.forPath(spark, "s3://databricks-wyseday-march-2024/JG/delta_NOAA")
deltaTable.optimize().executeCompaction()

// COMMAND ----------

// MAGIC %md
// MAGIC ## history

// COMMAND ----------

// MAGIC %md
// MAGIC ## vacuum

// COMMAND ----------

// MAGIC %md
// MAGIC documentation : https://docs.databricks.com/en/delta/vacuum.html#example-syntax-for-vacuum

// COMMAND ----------

// MAGIC %md
// MAGIC ici un exemple de bug sur le vacuum

// COMMAND ----------

// MAGIC %sql
// MAGIC VACUUM delta.`s3://databricks-wyseday-march-2024/JG/delta_NOAA` RETAIN 1 HOURS

// COMMAND ----------

// fix the bug with spark conf
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

// COMMAND ----------

// make vacuum on your table s3://databricks-wyseday-march-2024/INITIALES/delta_NOAA
%sql
VACUUM delta.`s3://databricks-wyseday-march-2024/JG/delta_NOAA` RETAIN 1 HOURS

// COMMAND ----------

// MAGIC %md
// MAGIC # via sql

// COMMAND ----------

// MAGIC %md
// MAGIC ### write in the catalog

// COMMAND ----------

// Register df into the catalog  initiale_noaa , ex : JG_noaa
noaa.createOrReplaceTempView("JG_nooa")

// COMMAND ----------

// MAGIC %md
// MAGIC ### explorer les schemas

// COMMAND ----------

// MAGIC %sql
// MAGIC show databases

// COMMAND ----------

// MAGIC %sql
// MAGIC USE default

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lire les données depuis le metastore

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from noaa limit 10 ;

// COMMAND ----------

// Read a data frame from the catalog
val sqlDf = spark.sql("SELECT * FROM JG_nooa;")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lire les données depuis s3

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM DELTA.`s3://databricks-wyseday-march-2024/JG/delta_NOAA` limit 10;
