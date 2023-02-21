# Databricks notebook source
babynames = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/workflow-repo/babynames.csv")

babynames.createOrReplaceTempView("babynames_table")

years = spark.sql("select distinct(Year) from babynames_table").rdd.map(lambda row : row[0]).collect()
years.sort()

dbutils.widgets.dropdown("year", "2014", [str(x) for x in years])

babynames_2014 = babynames.filter(babynames.Year == dbutils.widgets.get("year"))
babynames_2014.write.format("csv").option("header", "true").mode("overwrite").save("dbfs:/FileStore/workflow-repo/babynames-2014.csv")

# COMMAND ----------


