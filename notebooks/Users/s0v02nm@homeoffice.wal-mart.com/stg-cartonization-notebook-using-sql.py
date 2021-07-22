# Databricks notebook source
# MAGIC %md # Mounting Storage

# COMMAND ----------

# setting up

# mounting storage account 

#Application (Client) ID 
applicationId = dbutils.secrets.get(scope="cartonizationStorageaccScope",key="devcartonizationstrgacc-client-id") 

# Application (Client) Secret Key
authenticationKey = dbutils.secrets.get(scope="cartonizationStorageaccScope",key="devcartonizationClientSecretKey") 

# Directory (Tenant) ID 
tenantId = dbutils.secrets.get(scope="cartonizationStorageaccScope",key="devcartonizationstrgacc-tenant-id")

endpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token" 

configs = {"fs.azure.account.auth.type": "OAuth", 
           "fs.azure.account.oauth.provider.type": 
           "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", 
           "fs.azure.account.oauth2.client.id": applicationId, 
           "fs.azure.account.oauth2.client.secret": authenticationKey, 
           "fs.azure.account.oauth2.client.endpoint": endpoint} 

if not any(mount.mountPoint == "/mnt/transient" for mount in dbutils.fs.mounts()): 
  dbutils.fs.mount(
    source ="abfss://transient@devcartonizationstrgacc.dfs.core.windows.net/",
    mount_point = "/mnt/transient", 
    extra_configs = configs)

# COMMAND ----------

import json
from pyspark.sql.types import*

schema_json = '{"fields":[{"metadata":{},"name":"_attachments","nullable":true,"type":"string"},{"metadata":{},"name":"_etag","nullable":true,"type":"string"},{"metadata":{},"name":"_rid","nullable":true,"type":"string"},{"metadata":{},"name":"_self","nullable":true,"type":"string"},{"metadata":{},"name":"_ts","nullable":true,"type":"integer"},{"metadata":{},"name":"authUser","nullable":true,"type":{"fields":[{"metadata":{},"name":"userName","nullable":true,"type":"string"},{"metadata":{},"name":"email","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"cartons","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"cartonBarCode","nullable":true,"type":"string"},{"metadata":{},"name":"cartonCode","nullable":true,"type":"string"},{"metadata":{},"name":"cartonDimension","nullable":true,"type":{"fields":[{"metadata":{},"name":"length","nullable":true,"type":"double"},{"metadata":{},"name":"width","nullable":true,"type":"double"},{"metadata":{},"name":"height","nullable":true,"type":"double"}],"type":"struct"}},{"metadata":{},"name":"cartonID","nullable":true,"type":"string"},{"metadata":{},"name":"cartonState","nullable":true,"type":"string"},{"metadata":{},"name":"cartonType","nullable":true,"type":"string"},{"metadata":{},"name":"hazmatLabelsList","nullable":true,"type":{"fields":[{"metadata":{},"name":"3091","nullable":true,"type":"string"},{"metadata":{},"name":"3481","nullable":true,"type":"string"},{"metadata":{},"name":"LQTY","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"hazmatStatus","nullable":true,"type":"string"},{"metadata":{},"name":"orderId","nullable":true,"type":"string"},{"metadata":{},"name":"unitList","nullable":true,"type":{"containsNull":false,"elementType":{"fields":[{"metadata":{},"name":"alignment","nullable":true,"type":{"fields":[{"metadata":{},"name":"x","nullable":true,"type":"double"},{"metadata":{},"name":"y","nullable":true,"type":"double"},{"metadata":{},"name":"z","nullable":true,"type":"double"}],"type":"struct"}},{"metadata":{},"name":"dimension","nullable":true,"type":{"fields":[{"metadata":{},"name":"x","nullable":true,"type":"double"},{"metadata":{},"name":"y","nullable":true,"type":"double"},{"metadata":{},"name":"z","nullable":true,"type":"double"}],"type":"struct"}},{"metadata":{},"name":"gtins","nullable":true,"type":{"containsNull":false,"elementType":"string","type":"array"}},{"metadata":{},"name":"imageUrl","nullable":true,"type":"string"},{"metadata":{},"name":"itemName","nullable":true,"type":"string"},{"metadata":{},"name":"itemNumber","nullable":true,"type":"integer"},{"metadata":{},"name":"itemUpc","nullable":true,"type":"string"},{"metadata":{},"name":"placementPoint","nullable":true,"type":{"fields":[{"metadata":{},"name":"x","nullable":true,"type":"double"},{"metadata":{},"name":"y","nullable":true,"type":"double"},{"metadata":{},"name":"z","nullable":true,"type":"double"}],"type":"struct"}},{"metadata":{},"name":"validItemUpcs","nullable":true,"type":{"containsNull":false,"elementType":"string","type":"array"}}],"type":"struct"},"type":"array"}}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"containerId","nullable":true,"type":"string"},{"metadata":{},"name":"containerState","nullable":true,"type":"string"},{"metadata":{},"name":"containerType","nullable":true,"type":"string"},{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"items","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"gtins","nullable":true,"type":{"containsNull":false,"elementType":"string","type":"array"}},{"metadata":{},"name":"imageUrl","nullable":true,"type":"string"},{"metadata":{},"name":"itemName","nullable":true,"type":"string"},{"metadata":{},"name":"itemNumber","nullable":true,"type":"integer"},{"metadata":{},"name":"itemUpc","nullable":true,"type":"string"},{"metadata":{},"name":"total","nullable":true,"type":"integer"},{"metadata":{},"name":"validItemUpcs","nullable":true,"type":{"containsNull":false,"elementType":"string","type":"array"}}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"DateTime","nullable":true,"type":"timestamp"}],"type":"struct"}'

schema = StructType.fromJson(json.loads(schema_json))

# COMMAND ----------

# MAGIC %md 
# MAGIC **Time Window and Conversion**
# MAGIC 
# MAGIC * Purpose: Obtain 24 hour time window from from 12 AM to 11:59 PM PST everyday when job runs at 02:00 AM
# MAGIC 
# MAGIC * Time zone followed and recommended by Cosmos DB: UTC
# MAGIC 
# MAGIC * DateTimeAdd (<DateTimePart> , <numeric_expr> ,<DateTime>): Adds <numeric_expr> to the specified DateTime part [Read more](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-datetimeadd)
# MAGIC 
# MAGIC * TimestampToDateTime(c._ts*1000): Converts the "_ts" property of cosmos db into Datetime of format "yyyy-MM-ddTHH:mm:ss.fffffffZ"
# MAGIC [Read more](https://devblogs.microsoft.com/cosmosdb/new-date-and-time-system-functions/#:~:text=Converting%20the%20system%20_ts%20property%20to%20a%20DateTime%20string&text=To%20convert%20the%20_ts%20value,this%20value%20to%20a%20DateTime.)
# MAGIC   
# MAGIC * For job running at yyyy-MM-dd 02:00:00 (PST),
# MAGIC 
# MAGIC   DateTimeAdd('hh', -2, GetCurrentDateTime()) gives yyyy-MM-dd 00:00:00 PST 
# MAGIC 
# MAGIC   DateTimeAdd('hh', -26, GetCurrentDateTime()) gives yyyy-MM-(dd-1) 00:00:00 PST

# COMMAND ----------

# MAGIC %md # Reading from Cosmos DB

# COMMAND ----------

URI = "stg-cosmos-db"
# "https://dev-atlas-cartonization-cosmosdb.documents.azure.com:443/" 
PrimaryKey = "8a2dAo9Cgv9IXVuXF4QXGBlf079zHXqJeha5UHsVm18y9wxSmNs3N2cpSW8M4pLEa7WSYhlnFbQ3hMEGIUFS0Q==" 
CosmosDatabase = "carton-visualization" 
CosmosCollection = "ManualVisualizationResponse" 
 

query = "SELECT * FROM c WHERE (TimestampToDateTime(c._ts*1000) >= DateTimeAdd('hh', -26, GetCurrentDateTime()) AND TimestampToDateTime(c._ts*1000) < DateTimeAdd('hh', -2, GetCurrentDateTime()))" 

readConfig = { "Endpoint": URI, 
              "Masterkey": PrimaryKey, 
              "Database": CosmosDatabase, 
              "Collection": CosmosCollection,
              "mergeSchema": "True",
              "query_custom": query 
             } 

manualVisRespDF =(spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).schema(schema).load())

# COMMAND ----------

# MAGIC %md # Pre Processing

# COMMAND ----------

from pyspark.sql.functions import *
manualVisRespDF = manualVisRespDF.withColumn('DateTime', from_utc_timestamp(from_unixtime(manualVisRespDF._ts), 'UTC')) 


# COMMAND ----------

# MAGIC %md # Writing into Delta Lake

# COMMAND ----------

from delta.tables import*

# upserts
deltaTable = DeltaTable.forPath(spark, "/mnt/transient/ManualVisualizationResponse")
deltaTable.alias("manVisResTable").merge(
  manualVisRespDF.alias("df"),
  "manVisResTable.id = df.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------


