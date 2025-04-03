# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, FloatType

# Define schema for incoming telemetry data
schema = StructType() \
    .add("device_id", StringType()) \
    .add("temperature", FloatType()) \
    .add("timestamp", StringType())

# Set up the Spark configurations to access ADLS
spark.conf.set("fs.azure.account.key.<azure_storage_account_name>.dfs.core.windows.net","<storage_access_keys")

# Define the ADLS path where the processed data will be stored
adls_path = "abfss://telemetry-landing@<azure_storage_account_name>.dfs.core.windows.net/processed_data/"

# Set up Event Hubs connection parameters
eventhubs_connection_string = "<event_hubs_endpoint>"

# Encrypt the Event Hubs connection string
encrypted_connection_string = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventhubs_connection_string)

# Read data from Event Hubs
df = spark.readStream \
    .format("eventhubs") \
    .option("eventhubs.connectionString", encrypted_connection_string) \
    .load()

# Decode the data and apply schema
telemetry_df = df \
    .selectExpr("CAST(body AS STRING)") \
    .select(from_json("body", schema).alias("data")) \
    .select("data.*")

# Define checkpoint location
checkpoint_location = "/mnt/telemetry_checkpoint"

# Ensure checkpoint directory exists (optional)
dbutils.fs.mkdirs(checkpoint_location)

# Write the processed data to Azure Data Lake Storage in CSV format
query = telemetry_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", adls_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# Await termination to keep the streaming query active
query.awaitTermination()