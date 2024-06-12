# Databricks notebook source
dbutils.widgets.text("metaCatalogName", "")
dbutils.widgets.text("metaSchemaName", "")

# COMMAND ----------

CatalogName = dbutils.widgets.get("metaCatalogName")
SchemaName = dbutils.widgets.get("metaSchemaName")

# COMMAND ----------

# MAGIC %run "./functions/Functions"

# COMMAND ----------

if not catalog_exists(CatalogName):
    create_catalog(CatalogName)

# COMMAND ----------

if not schema_exists(CatalogName,SchemaName):
    create_schema(CatalogName,SchemaName)

# COMMAND ----------

if not table_exists(CatalogName,SchemaName,"meta_tablemappings"):
    create_meta_tablemappings(CatalogName,SchemaName)

# COMMAND ----------

if not table_exists(CatalogName,SchemaName,"meta_dataloadingmetrics"):
    create_meta_dataloadingmetrics(CatalogName,SchemaName)

# COMMAND ----------

if not table_exists(CatalogName,SchemaName,"meta_audittable"):
    create_meta_audittable(CatalogName,SchemaName)
