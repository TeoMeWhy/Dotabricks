# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import json
import requests
import time
from pyspark.sql.types import *
from pyspark.sql import functions as F
from delta.tables import *

from tqdm import tqdm

API_KEY = dbutils.secrets.get(scope="dota", key="api_key")

# COMMAND ----------

def get_data(match_id, **kwargs):
    url = f"https://api.opendota.com/api/matches/{match_id}"
    response = requests.get(url, params=kwargs)
    try:
        return response.json()
    except json.JSONDecodeError as err:
        print(err)
        return {}

def get_and_land_match_details(match_id):
    path = f"/mnt/datalake/raw/dota/pro_matches_landing/{match_id}.json"
    data = get_data(match_id=match_id, api_key=API_KEY)
    
    if "match_id" in data:
    
        data_str = json.dumps(data)
        try:
            dbutils.fs.put(path, data_str)

        except:
            print(f"O arquivo já existe: {data['match_id']}\n\n")
    else:
        print("Dando uma pausa...")
        time.sleep(10)
        print("Retornando...")
            
def get_pro_matches_ids():
    
    match_schema = StructType([StructField('match_id',LongType(), True),])
    
    df_history = (spark.read
                       .format("delta")
                       .load("/mnt/datalake/raw/dota/tb_pro_matches_stored")
                       .select("match_id")
                       .distinct())
    
    if "tb_pro_matches_proceeded" in [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/raw/dota/")]:
        print("Obtendo histórico do processo...")
        df_proceeded = ( spark.read
                              .format("delta")
                              .load("/mnt/datalake/raw/dota/tb_pro_matches_proceeded")
                              .withColumn("flag_proceeded", F.lit(1)) )
        
        return (df_history.join(df_proceeded, "match_id", "left")
                          .filter("flag_proceeded is null")
                          .select("match_id")
                          .orderBy("match_id", ascending=False)
                          .distinct())

    else:
        print("Histórico de processo não encontrado")
        return df_history.select("match_id").orderBy("match_id", ascending=False)

# COMMAND ----------

table_proceeded = "tb_pro_matches_proceeded"

schema = StructType([
    StructField("match_id", LongType(), False),
    StructField("start_time", LongType(), False),
])

tables = [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/raw/dota")]

if table_proceeded not in tables:
    df = spark.read.schema(schema).json("/mnt/datalake/raw/dota/pro_matches_landing")
    df.write.format("delta").save(f"/mnt/datalake/raw/dota/{table_proceeded}")

# COMMAND ----------

bronzeDeltaTable = DeltaTable.forPath(spark, f"/mnt/datalake/raw/dota/{table_proceeded}")

def upsertToDelta(df, batchId):
    (bronzeDeltaTable.alias("delta") 
                     .merge(df.alias("raw"), "raw.match_id = delta.match_id") 
                     .whenNotMatchedInsertAll()
                     .execute())

df_stream = (spark.readStream
                  .format('cloudFiles')
                  .option('cloudFiles.format', 'json')
                  .schema(schema)
                  .load("/mnt/datalake/raw/dota/pro_matches_landing"))

stream = (df_stream.writeStream
                   .foreachBatch(upsertToDelta)
                   .option('checkpointLocation', "/mnt/datalake/raw/dota/tb_pro_matches_proceeded_checkpoint")
                   .outputMode("update")
                   .start())

# COMMAND ----------

match_ids = get_pro_matches_ids()
for i in tqdm(match_ids.collect()):
    get_and_land_match_details(i[0])

# COMMAND ----------

stream.processAllAvailable()
stream.stop()

spark.sql("OPTIMIZE delta.`/mnt/datalake/raw/dota/tb_pro_matches_proceeded`")

bronzeDeltaTable.vacuum()
