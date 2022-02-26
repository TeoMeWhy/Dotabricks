# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import json
import requests
from pyspark.sql.types import *
from pyspark.sql import functions as F

from tqdm import tqdm

# COMMAND ----------

def get_data(match_id, **kwargs):
    url = f"https://api.opendota.com/api/matches/{match_id}"
    response = requests.get(url, params=kwargs)
    return response.json()

def get_and_land_match_details(match_id):
    path = f"/mnt/datalake/raw/pro_matches_landing/{match_id}.json"
    data = get_data(match_id=match_id)
    
    if "match_id" in data:
    
        data_str = json.dumps(data)
        try:
            dbutils.fs.put(path, data_str)
            ( spark.createDataFrame([{"match_id":match_id, "path":path}])
                   .write
                   .format("delta")
                   .mode("append")
                   .save("/mnt/datalake/raw/pro_matches_proceeded") )

        except:
            print("O arquivo já existe")
            
def get_pro_matches_ids():
    df_history = spark.read.format("delta").load("/mnt/datalake/bronze/pro_matches_history")
    
    if "pro_matches_proceeded" in [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/raw/")]:
        print("Verificando partidas coletadas")
        
        df_proceeded = ( spark.read
                              .format("delta")
                              .load("/mnt/datalake/raw/pro_matches_proceeded") )
        
        return ( df_history.join(df_proceeded, "match_id", "left")
                           .filter("path is null")
                           .select("match_id") )

    else:
        print("Histórico de processo não encontrado")
        return df_history.select("match_id")

# COMMAND ----------

match_ids = get_pro_matches_ids()

for i in tqdm(match_ids.collect()):
    get_and_land_match_details(i[0])
