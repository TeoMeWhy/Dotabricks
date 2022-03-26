# Databricks notebook source
import json
import os
import time

from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

def import_schema(table_name:str)->str:
    with open(f"schemas/{table_name}.json", "r") as open_file:
        schema = open_file.read()
    return schema

def get_data(schema):
    path = f"/mnt/datalake/raw/dota/pro_matches_landing"
    return spark.read.schema(schema).json(path)

def etl_match_player(df):
    df = (df.withColumn("players", F.explode("players") )
            .select("players.*")
            .withColumn("match_datetime", F.from_unixtime(F.col("start_time"), "yyyy-MM-dd")))
    return df

def etl_match(df):
    df = (df.withColumn("match_datetime", F.from_unixtime(F.col("start_time"), "yyyy-MM-dd")))
    return df

def upsert(df, batchId, etl, path):
    df = etl(df)
    df.write.format("delta").mode("append").save(path)
    
etls = {
    "tb_pro_match_player": etl_match_player,
    "tb_pro_match" : etl_match
}

# COMMAND ----------

tb_name = dbutils.widgets.get("table")
tb_path = f"/mnt/datalake/raw/dota/{tb_name}"
checkpoint_path = f"/mnt/datalake/raw/dota/{tb_name}_checkpoint"
schema = StructType.fromJson(json.loads(import_schema(tb_name)))

etl = etls[tb_name]

# COMMAND ----------

df_stream = (spark.readStream
                  .format('cloudFiles')
                  .option('cloudFiles.format', 'json')
                  .schema(schema)
                  .load("/mnt/datalake/raw/dota/pro_matches_landing"))

stream = (df_stream.writeStream
                   .foreachBatch(lambda df, batchId: upsert(df, batchId, etl, tb_path))
                   .option('checkpointLocation', checkpoint_path)
                   .outputMode("update")
                   .start())

# COMMAND ----------

time.sleep(60)
stream.processAllAvailable()
stream.stop()

# COMMAND ----------

df = spark.read.format("delta").load(tb_path)
df.display()
