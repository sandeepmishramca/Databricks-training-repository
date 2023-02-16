# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Using Conda
# MAGIC 
# MAGIC * Conda is one of the most widely-used Python package management systems.
# MAGIC 
# MAGIC * PySpark users can directly use a Conda environment to ship their third-party Python packages by leveraging conda-pack which is a command line tool creating relocatable Conda environments. 
# MAGIC 
# MAGIC * It is supported in all types of clusters in the current Apache Spark 3.3.0 or lower versions.

# COMMAND ----------

# MAGIC %conda create -y -n pyspark_conda_env -c conda-forge pyarrow pandas conda-pack

# COMMAND ----------

# MAGIC %conda activate pyspark_conda_env

# COMMAND ----------

# MAGIC %conda pack -f -o pyspark_conda_env.tar.gz
