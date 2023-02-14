# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Notebook-scoped Python libraries
# MAGIC 
# MAGIC Notebook-scoped libraries let you create, modify, save, reuse, and share custom Python environments that are specific to a notebook. When you install a notebook-scoped library, only the current notebook and any jobs associated with that notebook have access to that library. Other notebooks attached to the same cluster are not affected.
# MAGIC 
# MAGIC Notebook-scoped libraries do not persist across sessions. You must reinstall notebook-scoped libraries at the beginning of each session, or whenever the notebook is detached from a cluster.
# MAGIC 
# MAGIC There are two methods for installing notebook-scoped libraries:
# MAGIC 
# MAGIC * Run the %pip magic command in a notebook. Databricks recommends using this approach for new workloads. This article describes how to use these magic commands.
# MAGIC 
# MAGIC * On Databricks Runtime 10.5 and below, you can use the Databricks library utility. The library utility is supported only on Databricks Runtime, not Databricks Runtime ML or Databricks Runtime for Genomics.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Important
# MAGIC 
# MAGIC * We should place all %pip commands at the beginning of the notebook. The notebook state is reset after any %pip command that modifies the environment. If you create Python methods or variables in a notebook, and then use %pip commands in a later cell, the methods or variables are lost.
# MAGIC 
# MAGIC * Upgrading, modifying, or uninstalling core Python packages (such as IPython) with %pip may cause some features to stop working as expected. For example, IPython 7.21 and above are incompatible with Databricks Runtime 8.1 and below. If you experience such problems, reset the environment by detaching and re-attaching the notebook or by restarting the cluster.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Install a library with %pip

# COMMAND ----------

# MAGIC %pip install matplotlib

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install a wheel package with %pip

# COMMAND ----------

# MAGIC %pip install /path/to/my_package.whl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uninstall a library with %pip

# COMMAND ----------

# MAGIC %pip uninstall -y matplotlib

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install a library from a version control system with %pip

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databricks/databricks-cli

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install a private package with credentials managed by Databricks secrets with %pip

# COMMAND ----------

token = dbutils.secrets.get(scope="scope", key="key")

# COMMAND ----------

# MAGIC %pip install --index-url https://<user>:$token@<your-package-repository>.com/<path/to/repo> <package>==<version> --extra-index-url https://pypi.org/simple/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install a package from DBFS with %pip

# COMMAND ----------

# MAGIC %pip install /dbfs/mypackage-0.0.1-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save libraries in a requirements file

# COMMAND ----------

# MAGIC %pip freeze > /dbfs/requirements.txt

# COMMAND ----------

# MAGIC %cat /dbfs/requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use a requirements file to install libraries

# COMMAND ----------

# MAGIC %pip install -r /dbfs/requirements.txt
