# Databricks notebook source
df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/circuits.csv")



# COMMAND ----------


df.select(concat("location","country").alias("new column"))

df.select("*", concat("location",lit(" "),"country").alias("new column"))

# COMMAND ----------

df

# COMMAND ----------

ls

# COMMAND ----------

from pyspark.sql.functions import *
df.select(concat("location","country").alias("new column"))

# COMMAND ----------

from pyspark.sql.functions import *
df.select(concat("location","country").alias("new column")).display()

# COMMAND ----------

df.columns
newcolumns=['circuit_id',
 'circuit_ref',
 'firstname',
 'location',
 'country',
 'lat',
 'lng',
 'alt',
 'url']

# COMMAND ----------

df.toDF(*newcolumns).display()

# COMMAND ----------

df.select("*", concat("location",lit(" "),"country").alias("new column")).display()

# COMMAND ----------

help(df.withColumnRenamed)

df.select(col("circuitId").alias("circuit_id")).display()

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------


df.withColumn("formula1",lit("Formula1Data")).display()

# COMMAND ----------


df.withColumn("New_column",lit(current_date())).display()

# COMMAND ----------

df.withColumn("ingestiondate",current_timestamp()).display()

# COMMAND ----------

df.withColumn(
"Date"
,current_timestamp()).display()

# COMMAND ----------

df.where("circuitID=1").display()

# COMMAND ----------

df.filter(col("circuitID")==1).display()

# COMMAND ----------

df.filter("circuitID > 10 and country ='UK'").display()

# COMMAND ----------

df.filter((col("circuitID") > 10) & (col("country") =='UK')).display()

# COMMAND ----------

df.orderBy(desc("circuitID")).display()

# COMMAND ----------

df.orderBy(col("circuitID").desc()).display()

# COMMAND ----------

df.sort("country").select("country","location").display()

# COMMAND ----------

df.sort("country","location").select("country","location").display()

# COMMAND ----------

df.drop("url","alt").display()
