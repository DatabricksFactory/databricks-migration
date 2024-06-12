# Databricks notebook source
# MAGIC %run "./functions/Functions"

# COMMAND ----------

dbutils.widgets.text("metaCatalogName", "")
dbutils.widgets.text("metaSchemaName", "")

# COMMAND ----------

metaCatalogName = dbutils.widgets.get("metaCatalogName")
metaSchemaName = dbutils.widgets.get("metaSchemaName")

# COMMAND ----------

tableName = metaCatalogName +"."+ metaSchemaName + ".meta_dataloadingmetrics"
tableConfigs = spark.read.format("delta").table(tableName).filter("IncrementalLoadFlag").collect()

for config in tableConfigs:
    if not table_exists(
        config["DestinationCatalogName"],
        config["DestinationSchemaName"],
        config["DestinationTableName"],
    ):
        create_delta_table(
            config["DestinationCatalogName"],
            config["DestinationSchemaName"],
            config["DestinationTableName"],
        )
        
    print(
        load_delta_table(
            config["SourceDatabaseName"],
            config["SourceSchemaName"],
            config["SourceTableName"],
            config["DestinationCatalogName"],
            config["DestinationSchemaName"],
            config["DestinationTableName"],
            config["IncrementalLoadFlag"],
            config["ReRunFlag"],
            config["RunDays"],
            config["WatermarkColumn"],
        )
    )
