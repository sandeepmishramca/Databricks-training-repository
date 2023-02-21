# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Install the CLI

# COMMAND ----------

pip install databricks-cli

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update the CLI

# COMMAND ----------

pip install databricks-cli --upgrade

# COMMAND ----------

# MAGIC %md
# MAGIC To list the version of the CLI that is currently installed, run databricks --version (or databricks -v):

# COMMAND ----------

# MAGIC %sh databricks --version

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set up authentication

# COMMAND ----------

# MAGIC %sh databricks configure --token

# COMMAND ----------

# MAGIC %sh cat ~/.databrickscfg

# COMMAND ----------

# MAGIC %md
# MAGIC Enter your workspace URL, with the format https://<instance-name>.cloud.databricks.com. After completing the prompts, the access credentials are stored in the file ~/.databrickscfg on Unix, Linux, or macOS, or %USERPROFILE%\.databrickscfg on Windows

# COMMAND ----------

# MAGIC %sh databricks workspace ls /Users/souchakr@publicisgroupe.net

# COMMAND ----------

# MAGIC %md
# MAGIC #### Testing with connection profiles
# MAGIC 
# MAGIC The Databricks CLI configuration supports multiple connection profiles. The same installation of Databricks CLI can be used to make API calls on multiple Databricks workspaces.

# COMMAND ----------

# MAGIC %sh databricks configure --token --profile souchakr

# COMMAND ----------

# MAGIC %md
# MAGIC The .databrickscfg file  will now contain a corresonding profile entry

# COMMAND ----------

# MAGIC %sh cat ~/.databrickscfg

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test the connection profiles

# COMMAND ----------

# MAGIC %sh databricks workspace ls /Users/souchakr@publicisgroupe.net --profile souchakr

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Find out jobs list in databricks using CLI

# COMMAND ----------

# MAGIC %sh databricks jobs list --output JSON

# COMMAND ----------

# MAGIC %md
# MAGIC After installing the json parser

# COMMAND ----------

# MAGIC %sh databricks jobs list --output JSON | jq '.jobs[] | select(.job_id == 286438077591891) | .settings'

# COMMAND ----------

# MAGIC %sh databricks jobs run-now --job-id 286438077591891

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Find out clusters list in databricks using CLI

# COMMAND ----------

# MAGIC %sh databricks clusters list --output JSON | jq '[ .clusters[] | { name: .cluster_name, id: .cluster_id } ]'
