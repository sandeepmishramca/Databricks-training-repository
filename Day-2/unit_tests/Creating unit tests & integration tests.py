# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Running unit/ integration tests in notebooks
# MAGIC 
# MAGIC For Python, R, and Scala notebooks, common approaches include the following:
# MAGIC 
# MAGIC ## Store functions and their unit tests outside of notebooks.
# MAGIC 
# MAGIC * Benefits: You can call these functions with and outside of notebooks. Test frameworks are better designed to run tests outside of notebooks.
# MAGIC 
# MAGIC * Challenges: This approach is not supported for Scala notebooks. This approach requires Databricks Repos. This approach also increases the number of files to track and maintain.
# MAGIC 
# MAGIC ## Store functions in one notebook and their unit tests in a separate notebook.
# MAGIC 
# MAGIC * Benefits: These functions are easier to reuse across notebooks.
# MAGIC 
# MAGIC * Challenges: The number of notebooks to track and maintain increases. These functions cannot be used outside of notebooks. These functions can also be more difficult to test outside of notebooks.
# MAGIC 
# MAGIC ## Store functions and their unit tests within the same notebook.
# MAGIC 
# MAGIC * Benefits: Functions and their unit tests are stored within a single notebook for easier tracking and maintenance.
# MAGIC 
# MAGIC * Challenges: These functions can be more difficult to reuse across notebooks. These functions cannot be used outside of notebooks. These functions can also be more difficult to test outside of notebooks.
# MAGIC 
# MAGIC ##### For Python and R notebooks, Databricks recommends storing functions and their unit tests outside of notebooks. For Scala notebooks, Databricks recommends including functions in one notebook and their unit tests in a separate notebook.
# MAGIC 
# MAGIC ##### For SQL notebooks, Databricks recommends that you store functions as SQL user-defined functions (SQL UDFs) in your schemas (also known as databases). You can then call these SQL UDFs and their unit tests from SQL notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Write functions
# MAGIC 
# MAGIC This section describes a simple set of example functions that determine the following:
# MAGIC 
# MAGIC * Whether a table exists in a database.
# MAGIC       => Expected result is A boolean type
# MAGIC * Whether a column exists in a table.
# MAGIC       => Expected result is A boolean type
# MAGIC * How many rows exist in a column for a value within that column.
# MAGIC       => Expected result is A Integer type non negative number

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# Does the specified table exist in the specified database?
def tableExists(tableName, dbName):
    return spark.catalog.tableExists(f"{dbName}.{tableName}")

# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
    if columnName in dataFrame.columns:
        return True
    else:
        return False

# How many rows are there for the specified value in the specified column
# in the given DataFrame?
def numRowsInColumnForValue(dataFrame, columnName, columnValue):
    df = dataFrame.filter(col(columnName) == columnValue)
    return df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Call functions

# COMMAND ----------

from myfunctions import *

tableName   = ""
dbName      = ""
columnName  = ""
columnValue = ""

# If the table exists in the specified database...
if tableExists(tableName, dbName):
    df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")

    # And the specified column exists in that table...
    if columnExists(df, columnName):
    # Then report the number of rows for the specified value in that column.
        numRows = numRowsInColumnForValue(df, columnName, columnValue)

        print(f"There are {numRows} rows in '{tableName}' where '{columnName}' equals '{columnValue}'.")
    else:
        print(f"Column '{columnName}' does not exist in table '{tableName}' in schema (database) '{dbName}'.")
else:
     print(f"Table '{tableName}' does not exist in schema (database) '{dbName}'.") 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Write unit tests

# COMMAND ----------

import pytest
import pyspark
from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

tableName    = "diamonds"
dbName       = "default"
columnName   = "clarity"
columnValue  = "SI2"

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# Create fake data for the unit tests to run against.
# In general, it is a best practice to not run unit tests
# against functions that work with data in production.
schema = StructType([ \
  StructField("_c0",     IntegerType(), True), \
  StructField("carat",   FloatType(),   True), \
  StructField("cut",     StringType(),  True), \
  StructField("color",   StringType(),  True), \
  StructField("clarity", StringType(),  True), \
  StructField("depth",   FloatType(),   True), \
  StructField("table",   IntegerType(), True), \
  StructField("price",   IntegerType(), True), \
  StructField("x",       FloatType(),   True), \
  StructField("y",       FloatType(),   True), \
  StructField("z",       FloatType(),   True), \
])

data = [ (1, 0.23, "Ideal",   "E", "SI2", 61.5, 55, 326, 3.95, 3.98, 2.43 ), \
         (2, 0.21, "Premium", "E", "SI1", 59.8, 61, 326, 3.89, 3.84, 2.31 ) ]

df = spark.createDataFrame(data, schema)

# Does the table exist?
def test_tableExists():
    assert tableExists(tableName, dbName) is True

# Does the column exist?
def test_columnExists():
    assert columnExists(df, columnName) is True

# Is there at least one row for the value in the specified column?
def test_numRowsInColumnForValue():
    assert numRowsInColumnForValue(df, columnName, columnValue) > 0
