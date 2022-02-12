# Databricks notebook source
import json
import requests
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

def get_data(match_id, **kwargs):
    url = f"https://api.opendota.com/api/matches/{match_id}"
    response = requests.get(url, params=kwargs)
    return response.json()

def get_and_land_match_details(match_id):
    path = f"/mnt/datalake/raw/pro_matches_landing/{match_id}.json"
    data = get_data(match_id=match_id)
    data_str = json.dumps(data)
    try:
        dbutils.fs.put(path, data_str)
        ( spark.createDataFrame([{"match_id":match_id, "path":path}])
               .write
               .format("parquet")
               .mode("append")
               .save("/mnt/datalake/raw/pro_matches_proceeded") )

    except:
        print("O arquivo já existe")

# COMMAND ----------

tables = [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/raw/")]
tables

# COMMAND ----------

matches_ids = spark.read.format("delta").load("/mnt/datalake/bronze/pro_matches_history").select("match_id")
