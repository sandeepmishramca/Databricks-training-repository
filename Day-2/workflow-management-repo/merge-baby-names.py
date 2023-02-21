# Databricks notebook source
babynames_2014 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/workflow-repo/babynames-2014.csv")

babynames_2015 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/workflow-repo/babynames-2015.csv")

babynames_merged = babynames_2014.unionAll(babynames_2015)
babynames_merged.write.format("csv").option("header", "true").mode("overwrite").save("dbfs:/FileStore/workflow-repo/babynames-merged.csv")

# display(babynames_merged)

# COMMAND ----------


