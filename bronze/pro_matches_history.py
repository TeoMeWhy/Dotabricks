# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import window
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

TABLE_NAME = "pro_matches_history"

tables = [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/bronze")]

if TABLE_NAME not in tables:
    print("Realizando a primeira carga em bronze")
    df = spark.read.format("json").load(f"/mnt/datalake/raw/{TABLE_NAME}/")
    windowSpec  = window.Window.partitionBy("match_id").orderBy("start_time")
    df_new = ( df.withColumn("row_number",F.row_number().over(windowSpec))
                 .filter("row_number = 1"))

    df_new.write.mode("overwrite").format("delta").saveAsTable(f"bronze_dota.{TABLE_NAME}")
else:
    print("Tabela já existe em bronze")

# COMMAND ----------

stream_schema = StructType([StructField("dire_name",StringType(),True),
                            StructField("dire_score",LongType(),True),
                            StructField("dire_team_id",LongType(),True),
                            StructField("duration",LongType(),True),
                            StructField("league_name",StringType(),True),
                            StructField("leagueid",LongType(),True),
                            StructField("match_id",LongType(),True),
                            StructField("radiant_name",StringType(),True),
                            StructField("radiant_score",LongType(),True),
                            StructField("radiant_team_id",LongType(),True),
                            StructField("radiant_win",BooleanType(),True),
                            StructField("series_id",LongType(),True),
                            StructField("series_type",LongType(),True),
                            StructField("start_time",LongType(),True)])

# COMMAND ----------

bronzeDeltaTable = DeltaTable.forPath(spark, f"/mnt/datalake/bronze/{TABLE_NAME}")

def upsertToDelta(df, batchId):
    windowSpec  = window.Window.partitionBy("match_id").orderBy("start_time")
    df_new = ( df.withColumn("row_number",F.row_number().over(windowSpec))
                 .filter("row_number = 1"))
    
    ( bronzeDeltaTable.alias("delta") 
                      .merge(df_new.alias("raw"), "delta.match_id = raw.match_id") 
                      .whenMatchedUpdateAll()
                      .whenNotMatchedInsertAll()
                      .execute() )
    
df_stream = ( spark.readStream
                   .format('cloudFiles')
                   .option('cloudFiles.format', 'json')
                   .schema(stream_schema)
                   .load(f"/mnt/datalake/raw/{TABLE_NAME}/") )

stream = (df_stream.writeStream
                   .foreachBatch(upsertToDelta)
                   .option('checkpointLocation', f"/mnt/datalake/bronze/{TABLE_NAME}_checkpoint")
                   .outputMode("update")
                   .start() 
         )

# COMMAND ----------

stream.processAllAvailable()
stream.stop()
