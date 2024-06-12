# Databricks notebook source
dbutils.widgets.text("srcDatabaseName", "")
dbutils.widgets.text("srcSchemaName", "")
dbutils.widgets.text("srcTableName", "")
dbutils.widgets.text("destCatalogName", "")
dbutils.widgets.text("destSchemaName", "")
dbutils.widgets.text("destTableName", "")
dbutils.widgets.text("metaCatalogName", "")
dbutils.widgets.text("metaSchemaName", "")
dbutils.widgets.text("incLoadFlag", "")
dbutils.widgets.text("reRunFlag", "")
dbutils.widgets.text("runDays", "")
dbutils.widgets.text("watermarkColumn", "")

# COMMAND ----------

srcDatabaseName = dbutils.widgets.get("srcDatabaseName")
srcSchemaName = dbutils.widgets.get("srcSchemaName")
srcTableName = dbutils.widgets.get("srcTableName")
destCatalogName = dbutils.widgets.get("destCatalogName")
destSchemaName = dbutils.widgets.get("destSchemaName")
destTableName = dbutils.widgets.get("destTableName")
metaCatalogName = dbutils.widgets.get("metaCatalogName")
metaSchemaName = dbutils.widgets.get("metaSchemaName")
incLoadFlag = int(dbutils.widgets.get("incLoadFlag"))
reRunFlag = int(dbutils.widgets.get("reRunFlag"))
runDays = int(dbutils.widgets.get("runDays"))
watermarkColumn = dbutils.widgets.get("watermarkColumn")

for widget_name in [
    "srcDatabaseName",
    "srcSchemaName",
    "srcTableName",
    "destCatalogName",
    "destSchemaName",
    "destTableName",
    "metaCatalogName",
    "metaSchemaName",
    "incLoadFlag",
    "reRunFlag",
    "runDays",
]:
    widget_value = dbutils.widgets.get(widget_name)
    if not widget_value.strip():
        raise ValueError(f"A value must be provided for '{widget_name}'")

# COMMAND ----------

# MAGIC %run "./functions/Functions"

# COMMAND ----------

upsert_meta_tablemappings(
    srcDatabaseName,
    srcSchemaName,
    srcTableName,
    destCatalogName,
    destSchemaName,
    destTableName,
    metaCatalogName,
    metaSchemaName
)

# COMMAND ----------

upsert_meta_dataloadingmetrics(
    srcDatabaseName,
    srcSchemaName,
    srcTableName,
    destCatalogName,
    destSchemaName,
    destTableName,
    incLoadFlag,
    reRunFlag,
    runDays,
    watermarkColumn,
    metaCatalogName,
    metaSchemaName
)
