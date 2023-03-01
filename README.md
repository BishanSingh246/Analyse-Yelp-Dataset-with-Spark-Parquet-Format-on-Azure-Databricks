<h1 align="center">
    <b>Analyse Yelp Dataset with Spark & Parquet Format on Azure Databricks</b> 
<br>
</h1>

## Description

This repository will help you to understand how to use Azure Databricks to analyze Yelp Dataset which is stored in Azure Storage Accounts, such as Azure Data Lake Storage.
- You will learn:
  - How to connect your Azure storage account to a Databricks Notebook
  - How to change a JSON file to Delta and Parquet format
  - How to create a DataFrame from data uploaded to your Azure storage account
  - How to read a Parquet file
  - How to create a partition by using repartition and coalesce
  - How to extract the year and month from a Date Column
  - How to analyze the data using the DataFrame and SQL queries.


## Instruction

1. Download Yelp Dataset and uplode zip file in Azure Storage Account.
2. Download this repository on your local machine.
3. Open Azure Databrick and import these files.
4. You should provide your storage account name, access key, in the ```Auth.py``` file.


