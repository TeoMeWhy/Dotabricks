# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import json
import requests
import time
from pyspark.sql.types import *
from pyspark.sql import functions as F

from tqdm import tqdm

API_KEY = dbutils.secrets.get(scope="dota", key="api_key")

# COMMAND ----------

def get_data(match_id, **kwargs):
    url = f"https://api.opendota.com/api/matches/{match_id}"
    response = requests.get(url, params=kwargs)
    return response.json()

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
    
    df_history = spark.read.json("/mnt/datalake/raw/dota/pro_matches_history").distinct()
    
    if "pro_matches_landing" in [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/raw/dota/")]:
        
        df_proceeded = ( spark.read
                              .schema(match_schema)
                              .format("json")
                              .load("/mnt/datalake/raw/dota/pro_matches_landing")
                              .withColumn("flag_proceeded", F.lit(1)) )
        
        return ( df_history.join(df_proceeded, "match_id", "left")
                           .filter("flag_proceeded is null")
                           .select("match_id") )

    else:
        print("Histórico de processo não encontrado")
        return df_history.select("match_id")

# COMMAND ----------

match_ids = get_pro_matches_ids()

for i in tqdm(match_ids.collect()):
    get_and_land_match_details(i[0])
