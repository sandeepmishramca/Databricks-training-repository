# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('double')
def pandas_plus_one(v: pd.Series) -> pd.Series:
    return v + 1

spark.range(10).select(pandas_plus_one("id")).show()

# COMMAND ----------


