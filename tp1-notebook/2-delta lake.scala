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
val noaa = 

// COMMAND ----------

// ici vous devez pouvoir afficher les 10 premières lignes
display()

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
// MAGIC ici un exemple de bug sur le vacuum

// COMMAND ----------

// MAGIC %sql
// MAGIC  ? 1 HOURS

// COMMAND ----------

// fix the bug with spark conf
spark.conf.set(?)

// COMMAND ----------

// make vacuum on your table s3://databricks-wyseday-march-2024/INITIALES/delta_NOAA
%sql
? 1 HOURS

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
// MAGIC -- afficher les databases

// COMMAND ----------

// MAGIC %sql
// MAGIC -- set a database

// COMMAND ----------

// MAGIC %sql
// MAGIC -- afficher les tables

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lire les données depuis le metastore

// COMMAND ----------

// MAGIC %sql
// MAGIC -- lire la table que vous venez de créer

// COMMAND ----------

// autre méthode
val sqlDf = spark.sql("SELECT * FROM ?")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lire les données depuis s3

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM ? limit 10;
