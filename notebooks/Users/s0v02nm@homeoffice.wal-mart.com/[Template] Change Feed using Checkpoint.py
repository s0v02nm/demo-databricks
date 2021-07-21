# Databricks notebook source
# MAGIC %md # Change Feed using Checkpoint
# MAGIC 
# MAGIC **Purpose:** Write changes from Cosmos DB to Delta Lake. Recover data in case of job fails
# MAGIC 
# MAGIC **Prequisities:**
# MAGIC * A Cosmos DB account
# MAGIC * A Delta Lake storage account
# MAGIC * A check point location in the storage
# MAGIC * A copy of above check point location in the storage
# MAGIC 
# MAGIC **Steps:**
# MAGIC 
# MAGIC * Get JSON response of the job runs list
# MAGIC * Retrieve the status (success or failure) of last job
# MAGIC * If the last job status is successful, it means the previous changes have been successfully written in the delta lake storage. Update the checkpoint_location_copy by copying checkpoint_location
# MAGIC * Else, if the last job failed, it means changes have not been written in the delta lake storage. Rollback to the previous check point location by copying checkpoint_location_copy to checkpoint_location
# MAGIC * Write all the changes that occured since the last checkpoint_location
# MAGIC 
# MAGIC **Resources**
# MAGIC * [Generate Databricks Access Token](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication)
# MAGIC * [Get Databricks instnace](https://docs.microsoft.com/en-us/azure/databricks/workspace/workspace-details)
# MAGIC * [Databricks Jobs](https://docs.microsoft.com/en-us/azure/databricks/jobs)
# MAGIC * [Copy using Databricks Utilities](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
# MAGIC 
# MAGIC 
# MAGIC **Author:** Sakshi Vats
# MAGIC  

# COMMAND ----------

import json
import requests
URI = "<cosmos db URI>"
PrimaryKey = "<cosmos db primary key>" 
CosmosDatabase = "<cosmos db database name>" 
CosmosCollection = "<cosmos db collection name>"
Container = "<storage container name>"
Checkpoint = "<checkpoint location>"

# a function to a boolean based on whether the last notebook job was successful or not
def ifLastJobSuccessful():
  gethooks = "https://<databricks notebbok instance>/api/2.0/jobs/runs/list" #e.g. adb-6448907101732577.17.azuredatabricks.net
  headers = {"Authorization": "Bearer <databricks access token>"} 
  response = requests.get(gethooks, headers=headers)
  
  # stores the status of previous job in string 'status'
  status = response.json()['runs'][1]['state']['result_state']
  
  return (status=="SUCCESS")
  

recurse = True
# if last job was successful, it was a clean write. Create a backup of current check point location
# if last job failed, rollback to the previous clean check point location and try writing again
if(ifLastJobSuccessful()):
  dbutils.fs.cp('<checkpoint location path>', '<checkpoint location copy path>', recurse)

else:
  dbutils.fs.cp('<copy of checkpoint location path>', '<checkpoint location path>', recurse)
  
changeConfig = { 
  "Endpoint": URI, 
  "Masterkey": PrimaryKey, 
  "Database": CosmosDatabase, 
  "Collection": CosmosCollection, 
  "ReadChangeFeed": "true",
  "ChangeFeedQueryName": CosmosDatabase + CosmosCollection + " ",
  "ChangeFeedStartFromTheBeginning": "false",
  "ChangeFeedUseNextToken": "true",
  "ChangeFeedCheckpointLocation": Checkpoint, 
  "SamplingRatio": "1.0"
 }

# reading changes from change feed
changefeedDF = (spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**changeConfig).load()) #

changefeed_list= changefeedDF.collect()
if(len(changefeed_list)>0):
  queryDF= spark.createDataFrame(changefeed_list)
  config = { 
  "mergeSchema": "true"
  }
  
  # writing the changes into delta
  queryDF.write.format("delta").options(**config).mode("append").save("<delta storage path>")