# Databricks notebook source
dbutils.widgets.text("CatalogName", "")
dbutils.widgets.text("SchemaName", "")

# COMMAND ----------

CatalogName = dbutils.widgets.get("CatalogName")
SchemaName = dbutils.widgets.get("SchemaName")

# COMMAND ----------

# MAGIC %run "./functions/Functions"

# COMMAND ----------

sql_query = f"""
CREATE TABLE IF NOT EXISTS {CatalogName}.{SchemaName}.control_table (
    SourceDatabaseName STRING,
    SourceSchemaName STRING,
    SourceTableName STRING,
    DestinationCatalogName STRING,
    DestinationSchemaName STRING,
    DestinationTableName STRING,
    IncrementalLoadFlag BOOLEAN,
    ReRunFlag BOOLEAN,
    RunDays INT,
    WatermarkColumn STRING,
    PrimaryKeys STRING
)
USING delta
"""

# Execute the SQL query
spark.sql(sql_query)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

# COMMAND ----------

def populate_control_table(CatalogName: str,
    SchemaName: str):
    # Define the schema corresponding to the CSV file structure
 schema = StructType([
    StructField("SourceDatabaseName", StringType(), True),
    StructField("SourceSchemaName", StringType(), True),
    StructField("SourceTableName", StringType(), True),
    StructField("DestinationCatalogName", StringType(), True),
    StructField("DestinationSchemaName", StringType(), True),
    StructField("DestinationTableName", StringType(), True),
    StructField("IncrementalLoadFlag", BooleanType(), True),
    StructField("ReRunFlag", BooleanType(), True),
    StructField("RunDays", IntegerType(), True),
    StructField("WatermarkColumn", StringType(), True),
    StructField("PrimaryKeys", StringType(), True)
 ])
 # Path to the CSV file
 csv_file_path = "abfss://development@cwhdataengineering.dfs.core.windows.net/ControlTable/control_table_st.csv"
  # Read the CSV file into a DataFrame
 df = spark.read.schema(schema).csv(csv_file_path, header=True)

 table_name = CatalogName + '.' + SchemaName + '.control_table'
 # Write the DataFrame to the Delta table
 df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

populate_control_table (CatalogName,SchemaName)

# COMMAND ----------

process_control_table_to_meta_tablemapping(CatalogName,SchemaName)

# COMMAND ----------

process_control_table_to_meta_dataloading(CatalogName,SchemaName)
