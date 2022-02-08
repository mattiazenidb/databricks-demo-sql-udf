# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.oneenvadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.oneenvadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.oneenvadls.dfs.core.windows.net", dbutils.secrets.get(scope="mzeni-kv-tests",key="applicationid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.oneenvadls.dfs.core.windows.net", dbutils.secrets.get(scope="mzeni-kv-tests",key="secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.oneenvadls.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get(scope="mzeni-kv-tests",key="directoryid")))

# COMMAND ----------

dbutils.fs.rm('abfss://deltalake@oneenvadls.dfs.core.windows.net/mattia/demos/sql-udf/', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS database_mattia_demos_sql_udf CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE database_mattia_demos_sql_udf

# COMMAND ----------

def generate_user_names(faker):
  return faker.first_name(), faker.last_name()

# COMMAND ----------

import random
import string
from faker import Faker

def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))
  
def create_data():
  data = []
  faker = Faker()
  for index in range(0, 1000):
    first_name, last_name = generate_user_names(faker)
    age = random.randint(1,100)
    data.append([index, "{0} {1}".format(first_name, last_name), age, "{0}.{1}@gmail.com".format(first_name, last_name), "OLD" if age > 50 else "NOT OLD"])

  return spark.createDataFrame(data, ["ID", "NAME", "AGE", "EMAIL-PII", "LABEL"]).write.format("delta").save("abfss://deltalake@oneenvadls.dfs.core.windows.net/mattia/demos/sql-udf/delta-input/")

# COMMAND ----------

create_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE database_mattia_demos_sql_udf.dataset
# MAGIC   USING DELTA
# MAGIC   LOCATION 'abfss://deltalake@oneenvadls.dfs.core.windows.net/mattia/demos/sql-udf/delta-input/'
