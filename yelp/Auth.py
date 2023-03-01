# Databricks notebook source
# DBTITLE 1,Set ADLS(Azure Data Lake Storage) configuration
# spark.conf.set("fs.azure.account.key.<storage acount name>.dfs.core.windows.net","<storage account key>")
spark.conf.set("fs.azure.account.key.sayelp001.dfs.core.windows.net","SYgYLHxV150eyvx7mvbTg7vepEQi/Y66HoOwiod55sz+DeNcH4Lc8WJBocIeeAc4rqZjTkecRJPi+AStFjoWTg==")

# COMMAND ----------

# DBTITLE 1,List Datasets in ADLS
# dbutils.fs.ls("abfss://<container name>@<storage acount name>.dfs.core.windows.net/")
dbutils.fs.ls("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/")