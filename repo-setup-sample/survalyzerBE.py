# Databricks notebook source
import requests
from requests.structures import CaseInsensitiveDict
import json
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import *
from pyspark.sql.functions import array_contains

# COMMAND ----------

import requests
from requests.structures import CaseInsensitiveDict
import json

# get authentication token
#url = "https://xray.cloud.xpand-it.com/api/v2/authenticate"
url = "https://api.survalyzer-swiss.app/publicapi/Authentication/v1/GetApiToken"

headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"

#data = '{ "client_id": "9CA1D43B328C40FBAF77A77239F0B42F","client_secret": "520315d8166060e64a6b43fe1bfea36c531fa9282b2e30ff1ba900a3e311130b" }'
#data = '{ "client_id": "F699AB4FCEEB4D25B423738C17F6376C","client_secret": "bf7a62e72a5ceab996bb90709d193b22b4738a605babdbcf1aef58323cd52ad6" }'
data = '{"tenant":"sonepar", "userName":"edouard.clotilde_ext@sonepar.com","password":"Spark2021!"}'

tokenResponse = requests.post(url, headers=headers, data=data)
print(tokenResponse.status_code)
token = tokenResponse.content.decode(encoding='utf-8', errors= 'strict')
print(token)

# COMMAND ----------

headers = CaseInsensitiveDict()
#headers["Content-Type"] = "application/json"
headers["Authorization"] = "Bearer " + token[1:-1]
#url2 = 'https://api.survalyzer-swiss.app/publicapi/Interview/v2/ReadInterviewListCompact'
#url2 = 'https://sonepar.survalyzer-swiss.app/api/Survey/ReadSurvey'
url2 = 'https://sonepar.survalyzer-swiss.app/publicapi/Survey/v1/ReadSurveyDataCompact'

databe = '{"tenant":"sonepar","surveyId":"2467","conditions":[{"conjunction":"And","identifier":"State","conditionOperator":"IsEqualTo","value":"Completed","conditionType":"SQL"},{"conjunction":"Or","identifier":"State","conditionOperator":"IsEqualTo","value":"InProgress","conditionType":"SQL"}],"loadCodePlan":"true"}'

print("Survalyzer app called")
resbe = requests.post(url2, data=databe,headers=headers)
print(resbe.status_code)
resbe2 = resbe.content.decode(encoding='utf-8', errors= 'strict')
print(resbe2)

# COMMAND ----------

dbutils.fs.rm("/FileStore/sc/survalyzer/results",recurse= True)
dbutils.fs.put("/FileStore/sc/survalyzer/results/survalyzer_results_be.json", resbe2)
df = spark.read.option("multiline","true").json("/FileStore/sc/survalyzer/results/survalyzer_results_be.json")

# COMMAND ----------

from pyspark.sql.functions import array_contains
df1 = df.select(explode('rows').alias('results')).select("results.*")
display(df1,4)

# COMMAND ----------

#inprogress_count = df1.filter(col('status.InterviewState')).like('InProgress').count()

# COMMAND ----------

df1.write.format("delta").mode("overwrite").option("mergeschema","true").saveAsTable("Survalyzer_BE")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from Survalyzer_BE where InterviewState = 'Completed'

# COMMAND ----------


