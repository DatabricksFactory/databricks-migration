# Databricks notebook source
from pyspark.sql import types as T, functions as F, Row
from pyspark.sql.dataframe import DataFrame
from delta.tables import DeltaTable
import re
from pyspark.errors import AnalysisException

# COMMAND ----------

def load_sqlmi_table(databaseName: str, schemaName: str, tableName: str) -> DataFrame:
    #JDBC Connection to Dev MI - Run this first to create connection strings 
    jdbcHostName = ""
    jdbcDatabase = databaseName
    jdbcPort = 1433
    jdbcUsername = "" #User name and password are stored in Azure keyvault
    jdbcPassword = ""
    jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    jdbcUrl = f"jdbc:sqlserver://{jdbcHostName}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", f"{schemaName}.{tableName}")
        .load()
    )

    return df

# COMMAND ----------

@udf(returnType=T.StringType())
def get_delta_datatype(srcDb, dataType, precision, scale):
    sqlmi_to_delta_datatype_map  = {
        "bigint": "long",
        "binary": "binary",
        "bit": "boolean",
        "char": "string",
        "date": "date",
        "datetime": "timestamp",
        "datetime2": "timestamp",
        "datetimeoffset": "string",
        "decimal": f"decimal({precision}, {scale})",
        "float": "double",
        "geography": "string",
        "geometry": "string",
        "hierarchyid": "string",
        "image": "binary",
        "int": "int",
        "money": "decimal(19,4)",
        "nchar": "string",
        "ntext": "string",
        "numeric": f"decimal({precision}, {scale})",
        "nvarchar": "string",
        "real": "float",
        "smalldatetime": "timestamp",
        "smallint": "short",
        "smallmoney": "decimal(10,4)",
        "sql_variant": "string",
        "text": "string",
        "time": "string",
        "timestamp": "timestamp",
        "tinyint": "byte",
        "uniqueidentifier": "string",
        "varbinary": "binary",
        "varchar": "string",
        "xml": "string",
    }

    if (srcDb == "SQL MI"):
        return sqlmi_to_delta_datatype_map.get(dataType, 'Unknown')

# COMMAND ----------

def create_catalog(
    destCatalogName: str,
):
    ddl = f"""
    CREATE CATALOG {destCatalogName}
    """
    spark.sql(ddl)

# COMMAND ----------

def create_schema(
    destCatalogName: str,
    destSchemaName: str,
):
    ddl = f"""
    CREATE DATABASE {destCatalogName}.{destSchemaName}
    """
    spark.sql(ddl)

# COMMAND ----------

def create_meta_tablemappings(
    destCatalogName: str,
    destSchemaName: str,
):
    ddl = f"""
    CREATE TABLE {destCatalogName}.{destSchemaName}.meta_tablemappings (
        SourceDatabaseName STRING,
        SourceSchemaName STRING,
        SourceTableName STRING,
        DestinationCatalogName STRING,
        DestinationSchemaName STRING,
        DestinationTableName STRING,
        SourceColumnName STRING,
        DestinationColumnName STRING,
        SourceDataType STRING,
        DestinationDataType STRING
    ) USING DELTA;
    """
    spark.sql(ddl)

# COMMAND ----------

def create_meta_dataloadingmetrics(
    destCatalogName: str,
    destSchemaName: str,
):
    ddl = f"""
    CREATE TABLE {destCatalogName}.{destSchemaName}.meta_dataloadingmetrics (
        ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1),
        SourceDatabaseName STRING,
        SourceSchemaName STRING,
        SourceTableName STRING,
        DestinationCatalogName STRING,
        DestinationSchemaName STRING,
        DestinationTableName STRING,
        IncrementalLoadFlag BOOLEAN,
        ReRunFlag BOOLEAN,
        RunDays INT,
        WatermarkColumn STRING
    ) USING DELTA;
    """
    spark.sql(ddl)

# COMMAND ----------

def create_meta_audittable(
    destCatalogName: str,
    destSchemaName: str,
):
    ddl = f"""
    CREATE TABLE {destCatalogName}.{destSchemaName}.meta_audittable (
        ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1),
        DestinationCatalogName STRING,
        DestinationSchemaName STRING,
        DestinationTableName STRING,
        LastProcessedTime TIMESTAMP,
        SourceRowCount STRING,
        DestinationRowCount STRING
    ) USING DELTA;
    """
    spark.sql(ddl)

# COMMAND ----------

def create_delta_table(
    destCatalogName: str,
    destSchemaName: str,
    destTableName: str,
):
    ddl = get_ddl_string(destCatalogName, destSchemaName, destTableName)
    spark.sql(ddl)

# COMMAND ----------

 def get_ddl_string(
     destCatalogName: str,
     destSchemaName: str,
     destTableName: str,
 ) -> str:
     columns = (
         spark.read.format("delta")
         .table(f"{destCatalogName}.{destSchemaName}.meta_tablemappings")
         .filter(
             (F.col("DestinationCatalogName") == destCatalogName)
             & (F.col("DestinationSchemaName") == destSchemaName)
             & (F.col("DestinationTableName") == destTableName)
         )
         .collect()
     )

     ddl = (
         f"CREATE TABLE {destCatalogName}.{destSchemaName}.{destTableName} ("
     )
     for i, column in enumerate(columns):
         if i != len(columns) - 1:
             ddl += f"{column['DestinationColumnName']} {column['DestinationDataType']},"
         else:
             ddl += f"{column['DestinationColumnName']} {column['DestinationDataType']}"
     ddl += ") USING DELTA;"

     return ddl




# COMMAND ----------

def upsert_meta_tablemappings(
    srcDatabaseName: str,
    srcSchemaName: str,
    srcTableName: str,
    destCatalogName: str,
    destSchemaName: str,
    destTableName: str,
    metaCatalogName: str,
    metaSchemaName: str,
) -> dict:
    df = load_sqlmi_table(srcDatabaseName, "INFORMATION_SCHEMA", "COLUMNS")

    df = (
        df.filter(
            (F.col("Table_Catalog") == srcDatabaseName)
            & (F.col("Table_Schema") == srcSchemaName)
            & (F.col("Table_Name") == srcTableName)
        )
        .withColumnRenamed("Table_Catalog", "SourceDatabaseName")
        .withColumnRenamed("Table_Schema", "SourceSchemaName")
        .withColumnRenamed("Table_Name", "SourceTableName")
        .withColumnRenamed("Column_Name", "SourceColumnName")
        .withColumnRenamed("Data_Type", "SourceDataType")
        .withColumn(
            "DestinationColumnName",
            F.regexp_replace(F.col("SourceColumnName"), "[ /()&?$]", ""),
        )
       .withColumn(
            "DestinationColumnName",
            F.regexp_replace(F.col("DestinationColumnName"),"[%]","perc"),
        )
        .withColumn(
            "DestinationColumnName",
            F.regexp_replace(F.col("DestinationColumnName"), "[-.]", "_"),
        )
        .withColumn(
            "DestinationDataType",
            get_delta_datatype(
                F.lit("SQL MI"), "SourceDataType", "NUMERIC_PRECISION", "NUMERIC_SCALE"
            ),
        )
        .withColumn("DestinationCatalogName", F.lit(destCatalogName).cast("string"))
        .withColumn("DestinationSchemaName", F.lit(destSchemaName).cast("string"))
        .withColumn("DestinationTableName", F.lit(destTableName).cast("string"))
        .select(
            "SourceDatabaseName",
            "SourceSchemaName",
            "SourceTableName",
            "SourceColumnName",
            "SourceDataType",
            "DestinationColumnName",
            "DestinationDataType",
            "DestinationCatalogName",
            "DestinationSchemaName",
            "DestinationTableName",
        )
    )

    keyColumns = [
        "SourceDatabaseName",
        "SourceSchemaName",
        "SourceTableName",
        "SourceColumnName",
    ]

    updateColumns = [
        "DestinationColumnName",
        "DestinationDataType",
        "DestinationCatalogName",
        "DestinationSchemaName",
        "DestinationTableName",
    ]

    delta_table = DeltaTable.forName(
        spark, f"{metaCatalogName}.{metaSchemaName}.meta_tablemappings"
    )

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            " and ".join(f"target.{c} = source.{c}" for c in keyColumns),
        )
        .whenMatchedUpdateAll(
            " or ".join([f"target.`{c}` != source.`{c}`" for c in updateColumns])
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    return get_record_stats(destCatalogName, destSchemaName, "meta_tablemappings")

# COMMAND ----------

def upsert_meta_dataloadingmetrics(
    srcDatabaseName: str,
    srcSchemaName: str,
    srcTableName: str,
    destCatalogName: str,
    destSchemaName: str,
    destTableName: str,
    incLoadFlag: int,
    reRunFlag: int,
    runDays: int,
    watermarkColumn: str,
    metaCatalogName: str,
    metaSchemaName: str,
) -> dict:

    schema = T.StructType(
        [
            T.StructField("SourceDatabaseName", T.StringType(), True),
            T.StructField("SourceSchemaName", T.StringType(), True),
            T.StructField("SourceTableName", T.StringType(), True),
            T.StructField("DestinationCatalogName", T.StringType(), True),
            T.StructField("DestinationSchemaName", T.StringType(), True),
            T.StructField("DestinationTableName", T.StringType(), True),
            T.StructField("IncrementalLoadFlag", T.BooleanType(), True),
            T.StructField("ReRunFlag", T.BooleanType(), True),
            T.StructField("RunDays", T.IntegerType(), True),
            T.StructField("WatermarkColumn", T.StringType(), True),
        ]
    )

    if watermarkColumn is not None:
        watermarkColumn = str(re.sub(r"[ /()&]", "", watermarkColumn))
        watermarkColumn = str(watermarkColumn.replace("-", "_"))
    else:
        watermarkColumn = str(None)

    data = [
        Row(
            srcDatabaseName,
            srcSchemaName,
            srcTableName,
            destCatalogName,
            destSchemaName,
            destTableName,
            bool(incLoadFlag),
            bool(reRunFlag),
            int(runDays),
            watermarkColumn,
        ),
    ]

    df = spark.createDataFrame(data, schema)

    keyColumns = [
        "SourceDatabaseName",
        "SourceSchemaName",
        "SourceTableName",
    ]

    updateColumns = [
        "DestinationCatalogName",
        "DestinationSchemaName",
        "DestinationTableName",
        "IncrementalLoadFlag",
        "ReRunFlag",
        "RunDays",
        "WatermarkColumn",
    ]

    valuesColumns = {
        f"target.{col}": f"source.{col}" for col in keyColumns + updateColumns
    }
    
    setColumns = {f"target.{col}": f"source.{col}" for col in updateColumns}

    delta_table = DeltaTable.forName(
        spark, f"{metaCatalogName}.{metaSchemaName}.meta_dataloadingmetrics"
    )

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            " and ".join(f"target.{c} = source.{c}" for c in keyColumns),
        )
        .whenMatchedUpdate(
            " or ".join([f"target.`{c}` != source.`{c}`" for c in updateColumns]),
            set=setColumns,
        )
        .whenNotMatchedInsert(values=valuesColumns)
        .execute()
    )

    return get_record_stats(destCatalogName, destSchemaName, "meta_dataloadingmetrics")

# COMMAND ----------

def upsert_meta_audittable(destCatalogName: str, destSchemaName: str, destTableName: str, srcRowCount: int):
    delta_table = DeltaTable.forName(
        spark, f"{destCatalogName}.{destSchemaName}.{destTableName}"
    )

    lastProcessedTime = delta_table.history(1).first().timestamp

    destRowCount = delta_table.toDF().count()

    schema = T.StructType([
        T.StructField("DestinationCatalogName", T.StringType(), True),
        T.StructField("DestinationSchemaName", T.StringType(), True),
        T.StructField("DestinationTableName", T.StringType(), True),
        T.StructField("LastProcessedTime", T.TimestampType(), True),
        T.StructField("SourceRowCount", T.StringType(), True),
        T.StructField("DestinationRowCount", T.StringType(), True)
    ])

    data = [
        Row(destCatalogName, destSchemaName, destTableName, lastProcessedTime, srcRowCount, destRowCount),
    ]

    df = spark.createDataFrame(data, schema)

    keyColumns = [
        "DestinationCatalogName",
        "DestinationSchemaName",
        "DestinationTableName",
    ]

    updateColumns = [
        "LastProcessedTime",
        "SourceRowCount",
        "DestinationRowCount",
    ]

    valuesColumns = {
        f"target.{col}": f"source.{col}" for col in keyColumns + updateColumns
    }
    
    setColumns = {f"target.{col}": f"source.{col}" for col in updateColumns}

    meta_auditTable = DeltaTable.forName(
        spark, f"{destCatalogName}.{destSchemaName}.meta_audittable"
    )

    (
        meta_auditTable.alias("target")
        .merge(
            df.alias("source"),
            " and ".join(f"target.{c} = source.{c}" for c in keyColumns),
        )
        .whenMatchedUpdate(
            " or ".join([f"target.`{c}` != source.`{c}`" for c in updateColumns]),
            set=setColumns,
        )
        .whenNotMatchedInsert(values=valuesColumns)
        .execute()
    )

# COMMAND ----------

def load_delta_table(
    srcDatabaseName: str,
    srcSchemaName: str,
    srcTableName: str,
    destCatalogName: str,
    destSchemaName: str,
    destTableName: str,
    incLoadFlag: bool,
    reRunFlag: bool,
    runDays: int,
    watermarkColumn: str,
) -> dict:
    if incLoadFlag:
        return incremental_load(
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
        )
    else:
        return full_load(
            srcDatabaseName,
            srcSchemaName,
            srcTableName,
            destCatalogName,
            destSchemaName,
            destTableName,
            incLoadFlag,
            reRunFlag,
            runDays,
        )

# COMMAND ----------

def incremental_load(
    srcDatabaseName: str,
    srcSchemaName: str,
    srcTableName: str,
    destCatalogName: str,
    destSchemaName: str,
    destTableName: str,
    incLoadFlag: bool,
    reRunFlag: bool,
    runDays: int,
    watermarkColumn: str,
) -> dict:

    targetCount = (
        spark.read.format("delta")
        .table(f"{destCatalogName}.{destSchemaName}.{destTableName}")
        .count()
    )

    if targetCount == 0 or reRunFlag:
        return full_load(
            srcDatabaseName,
            srcSchemaName,
            srcTableName,
            destCatalogName,
            destSchemaName,
            destTableName,
            incLoadFlag,
            reRunFlag,
            runDays,
        )
    else:
        runDaysDate = spark.sql(
            f"SELECT CAST(REPLACE(current_date() - {runDays}, '-', '') AS INT)"
        ).first()[0]

        spark.sql(
            f"DELETE FROM {destCatalogName}.{destSchemaName}.{destTableName} WHERE {watermarkColumn} >= {runDaysDate}"
        )

        df = load_sqlmi_table(srcDatabaseName, srcSchemaName, srcTableName).filter(
            F.col(f"{watermarkColumn}") >= runDaysDate
        )

        columnTypeMapping = (
            spark.read.format("delta")
            .table(f"{destCatalogName}.{destSchemaName}.meta_tablemappings")
            .filter(
                (F.col("DestinationCatalogName") == destCatalogName)
                & (F.col("DestinationSchemaName") == destSchemaName)
                & (F.col("DestinationTableName") == destTableName)
            )
            .select(
                "SourceColumnName",
                "DestinationColumnName",
                "DestinationDataType",
            )
            .collect()
        )

        selectCols = list()

        for c in columnTypeMapping:
            df = df.withColumn(
                c["DestinationColumnName"],
                F.col(c["SourceColumnName"]).cast(c["DestinationDataType"]),
            )
            selectCols.append(c["DestinationColumnName"])

        try:
            df.count()
        except Exception as e:
            df = load_sqlmi_table(srcDatabaseName, srcSchemaName, srcTableName)
            for c in columnTypeMapping:
                df = df.withColumnRenamed(
                    c["SourceColumnName"],
                    c["DestinationColumnName"],
                )
            try:
                df.count()
            except Exception as e:
                return "[ERROR]: Datatype casting and column renaming failed."

        df.select(selectCols).write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(f"{destCatalogName}.{destSchemaName}.{destTableName}")

        srcRowCount = df.count()

        upsert_meta_audittable(
            destCatalogName, destSchemaName, destTableName, srcRowCount
        )

        return get_record_stats(destCatalogName, destSchemaName, destTableName)

# COMMAND ----------

def full_load(
    srcDatabaseName: str,
    srcSchemaName: str,
    srcTableName: str,
    destCatalogName: str,
    destSchemaName: str,
    destTableName: str,
    incLoadFlag: bool,
    reRunFlag: bool,
    runDays: int,
) -> dict:

    targetCount = (
        spark.read.format("delta")
        .table(f"{destCatalogName}.{destSchemaName}.{destTableName}")
        .count()
    )

    if targetCount == 0 or reRunFlag:
        df = load_sqlmi_table(srcDatabaseName, srcSchemaName, srcTableName)

        columnTypeMapping = (
            spark.read.format("delta")
            .table(f"{destCatalogName}.{destSchemaName}.meta_tablemappings")
            .filter(
                (F.col("DestinationCatalogName") == destCatalogName)
                & (F.col("DestinationSchemaName") == destSchemaName)
                & (F.col("DestinationTableName") == destTableName)
            )
            .select(
                "SourceColumnName",
                "DestinationColumnName",
                "DestinationDataType",
            )
            .collect()
        )

        selectCols = list()

        for c in columnTypeMapping:
            df = df.withColumn(
                c["DestinationColumnName"],
                F.col(c["SourceColumnName"]).cast(c["DestinationDataType"]),
            )
            selectCols.append(c["DestinationColumnName"])

        try:
            df.count()
        except Exception as e:
            df = load_sqlmi_table(srcDatabaseName, srcSchemaName, srcTableName)
            for c in columnTypeMapping:
                df = df.withColumnRenamed(
                    c["SourceColumnName"],
                    c["DestinationColumnName"],
                )
            try:
                df.count()
            except Exception as e:
                return "[ERROR]: Datatype casting and column renaming failed."

        df.select(selectCols).write.format("delta").mode("overwrite").option(
            "ōverwriteSchema", "true"
        ).saveAsTable(f"{destCatalogName}.{destSchemaName}.{destTableName}")

        srcRowCount = df.count()

        upsert_meta_audittable(
            destCatalogName, destSchemaName, destTableName, srcRowCount
        )

        return get_record_stats(destCatalogName, destSchemaName, destTableName)
    else:
        return {
            "table": f"{destCatalogName}.{destSchemaName}.{destTableName}",
            "sourceRows": 0,
            "rowsInserted": 0,
            "rowsUpdated": 0,
            "rowsDeleted": 0,
        }

# COMMAND ----------

def catalog_exists(catalog: str) -> bool:
    catalog_exists = (
        spark.sql("SHOW CATALOGS").filter(f"catalog = '{catalog}'").count() > 0
    )

    return catalog_exists

# COMMAND ----------

def schema_exists(catalog: str, schema: str) -> bool:
    schema_exists = (
        spark.sql(f"SHOW DATABASES IN {catalog}")
        .filter(f"databaseName = '{schema}'")
        .count()
        > 0
    )

    return schema_exists

# COMMAND ----------

def table_exists(catalog: str, schema: str, table: str) -> bool:
    
    catalog_exists = (
        spark.sql("SHOW CATALOGS").filter(f"catalog = '{catalog}'").count() > 0
    )

    if not catalog_exists:
        return False
    
    schema_exists = (
        spark.sql(f"SHOW DATABASES IN {catalog}")
        .filter(f"databaseName = '{schema}'")
        .count()
        > 0
    )

    if not schema_exists:
        return False
    
    table_exists = (
        spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        .filter(f"tableName = '{table.lower()}'")
        .count()
        > 0
    )

    return table_exists

# COMMAND ----------

def get_record_stats(
    destCatalogName: str, destSchemaName: str, destTableName: str
) -> dict:
    
    delta_table = DeltaTable.forName(
        spark, f"{destCatalogName}.{destSchemaName}.{destTableName}"
    )

    recordStats = delta_table.history(1).first()

    if recordStats.operation == "CREATE OR REPLACE TABLE AS SELECT":
        return {
            "table": f"{destCatalogName}.{destSchemaName}.{destTableName}",
            "version": recordStats.version,
            "sourceRows": recordStats.operationMetrics["numOutputRows"],
            "rowsInserted": recordStats.operationMetrics["numOutputRows"],
            "rowsUpdated": 0,
            "rowsDeleted": 0,
        }
    elif recordStats.operation == "MERGE":
        return {
            "table": f"{destCatalogName}.{destSchemaName}.{destTableName}",
            "version": recordStats.version,
            "sourceRows": recordStats.operationMetrics["numSourceRows"],
            "rowsInserted": recordStats.operationMetrics["numTargetRowsInserted"],
            "rowsUpdated": recordStats.operationMetrics["numTargetRowsUpdated"],
            "rowsDeleted": recordStats.operationMetrics["numTargetRowsDeleted"],
        }
    elif recordStats.operation == "WRITE":
        return {
            "table": f"{destCatalogName}.{destSchemaName}.{destTableName}",
            "version": recordStats.version,
            "sourceRows": recordStats.operationMetrics["numOutputRows"],
            "rowsInserted": recordStats.operationMetrics["numOutputRows"],
            "rowsUpdated": 0,
            "rowsDeleted": 0,
        }
    else:
        return {
            "table": f"{destCatalogName}.{destSchemaName}.{destTableName}",
            "version": recordStats.version,
            "sourceRows": -1,
            "rowsInserted": -1,
            "rowsUpdated": -1,
            "rowsDeleted": -1,
            "error": "Unknown operation",
        }

# COMMAND ----------

# Function to loop through the control table and execute upserts
def process_control_table_to_meta_tablemapping(CatalogName: str,
    SchemaName: str):
    # Load the control table
    table_name = CatalogName + '.' + SchemaName + '.control_table'
    control_df = spark.read.table(table_name)
    
    # Iterate through each row in the DataFrame
    results = []
    for row in control_df.collect():
        result = upsert_meta_tablemappings(
            srcDatabaseName=row['SourceDatabaseName'],
            srcSchemaName=row['SourceSchemaName'],
            srcTableName=row['SourceTableName'],
            destCatalogName=row['DestinationCatalogName'],
            destSchemaName=row['DestinationSchemaName'],
            destTableName=row['DestinationTableName'],
            metaCatalogName=row['DestinationCatalogName'],  # Assuming the metadata catalog is the same as the destination
            metaSchemaName=row['DestinationSchemaName']
        )
        results.append(result)

    return results

# COMMAND ----------

# Function to loop through the control table and execute upserts
def process_control_table_to_meta_dataloading(CatalogName: str,
    SchemaName: str):
    # Load the control table
    table_name = CatalogName + '.' + SchemaName + '.control_table'
    control_df = spark.read.table(table_name)
    
    # Iterate through each row in the DataFrame
    results = []
    for row in control_df.collect():
        result = upsert_meta_dataloadingmetrics(
            srcDatabaseName=row['SourceDatabaseName'],
            srcSchemaName=row['SourceSchemaName'],
            srcTableName=row['SourceTableName'],
            destCatalogName=row['DestinationCatalogName'],
            destSchemaName=row['DestinationSchemaName'],
            destTableName=row['DestinationTableName'],
            incLoadFlag=row['IncrementalLoadFlag'],
            reRunFlag=row['ReRunFlag'],
            runDays=row['RunDays'],
            watermarkColumn=row['WatermarkColumn'],
            metaCatalogName=row['DestinationCatalogName'],  # Assuming the metadata catalog is the same as the destination
            metaSchemaName=row['DestinationSchemaName']
        )
        results.append(result)

    return results

# COMMAND ----------

def incremental_load_withdelta_withmerge(
    srcDatabaseName: str,
    srcSchemaName: str,
    srcTableName: str,
    destCatalogName: str,
    destSchemaName: str,
    destTableName: str,
    incLoadFlag: bool,
    reRunFlag: bool,
    runDays: int,
    watermarkColumn: str,
) -> dict:

    targetCount = (
        spark.read.format("delta")
        .table(f"{destCatalogName}.{destSchemaName}.{destTableName}")
        .count()
    )

    if targetCount == 0 or reRunFlag:
        return full_load(
            srcDatabaseName,
            srcSchemaName,
            srcTableName,
            destCatalogName,
            destSchemaName,
            destTableName,
            incLoadFlag,
            reRunFlag,
            runDays,
        )
    else:
        LastProcessedTime = spark.sql(
            f"select max(`LastProcessedTime`) from cerner.prod.meta_audittable where `DestinationTableName` = destTableName"
        ).first()[0]


        df = load_sqlmi_table(srcDatabaseName, srcSchemaName, srcTableName).filter(
            F.col(f"{watermarkColumn}") >= LastProcessedTime
        )

        columnTypeMapping = (
            spark.read.format("delta")
            .table(f"{destCatalogName}.{destSchemaName}.meta_tablemappings")
            .filter(
                (F.col("DestinationCatalogName") == destCatalogName)
                & (F.col("DestinationSchemaName") == destSchemaName)
                & (F.col("DestinationTableName") == destTableName)
            )
            .select(
                "SourceColumnName",
                "DestinationColumnName",
                "DestinationDataType",
            )
            .collect()
        )

        selectCols = list()

        for c in columnTypeMapping:
            df = df.withColumn(
                c["DestinationColumnName"],
                F.col(c["SourceColumnName"]).cast(c["DestinationDataType"]),
            )
            selectCols.append(c["DestinationColumnName"])

        try:
            df.count()
        except Exception as e:
            df = load_sqlmi_table(srcDatabaseName, srcSchemaName, srcTableName)
            for c in columnTypeMapping:
                df = df.withColumnRenamed(
                    c["SourceColumnName"],
                    c["DestinationColumnName"],
                )
            try:
                df.count()
            except Exception as e:
                return "[ERROR]: Datatype casting and column renaming failed."
        
        keyColumns = []
            
        keyColumns = spark.sql(
            f"select `PrimaryKeys` from cerner.prod.control_table where `DestinationTableName` = destTableName"
        ).first()[0]
        
        delta_table = DeltaTable.forName(
        spark, f"{destCatalogName}.{destSchemaName}.{destTableName}"
        )

        if not keyColumns:
         (
          delta_table.alias("target")
          .merge(
            df.select(selectCols).alias("source"),
            " and ".join(f"target.{c} = source.{c}" for c in keyColumns),
            )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute()
         )
                    
        else:
           df.select(selectCols).write.format("delta").mode("append").option(
            "mergeSchema", "true"
           ).saveAsTable(f"{destCatalogName}.{destSchemaName}.{destTableName}")

        srcRowCount = df.count()

        upsert_meta_audittable(
            destCatalogName, destSchemaName, destTableName, srcRowCount
        )

        return get_record_stats(destCatalogName, destSchemaName, destTableName)
