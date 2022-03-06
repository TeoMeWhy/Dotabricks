# Databricks notebook source
import datetime
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("dire_name",StringType(),True),
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
    StructField("start_time",LongType(),True),
])


# COMMAND ----------

def get_data(**kwargs):
    url = "https://api.opendota.com/api/proMatches"
    response = requests.get(url, params=kwargs)
    return response.json()

def get_min_match_id(df):
    min_match_id = (df.groupBy()
                      .agg(F.min("match_id"))
                      .collect()[0][0])
    return min_match_id

def get_max_date(df, minus_days=0):
    max_date = (df.withColumn("match_date", F.from_unixtime("start_time"))
                  .groupBy()
                  .agg(F.date_add(F.max(F.col("match_date")),minus_days))
                  .collect()[0][0])
    return max_date

def get_min_date(df):
    min_date = (df.withColumn("match_date", F.from_unixtime("start_time"))
                  .groupBy()
                  .agg(F.date_add(F.min(F.col("match_date")),0))
                  .collect()[0][0])
    return min_date

def save_match_list(df):
    (df.coalesce(1)
       .write
       .format("json")
       .mode("append")
       .save("/mnt/datalake/raw/dota/pro_matches_history"))
    
def get_and_save(**kwargs):
    data = get_data(**kwargs) # obtem partidas novas a partir da partida mais antiga
    if len(data) != 0:
        df = spark.createDataFrame(data, schema=schema) # transforma em df spark
        save_match_list(df) # salva os dados em modo append
        return df
    else:
        return None

def get_history_pro_matches(**kwargs):
    df = spark.read.format("json").load("/mnt/datalake/raw/dota/pro_matches_history") # lê os dados do datalake
    min_match_id = get_min_match_id(df)
    while min_match_id is not None:
        
        print(min_match_id)
        df_new = get_and_save(less_than_match_id=min_match_id, **kwargs)
        if df_new == None:
            break
        min_match_id = get_min_match_id(df_new)
        
    print("Coleta finalizada!")
            
def get_new_pro_matches(since, **kwargs):
    df = spark.read.format("json").load("/mnt/datalake/raw/dota/pro_matches_history") # lê os dados do datalake
    max_date = get_max_date(df, since)        # data da partida mais recente que já foi coletada
    df_new = get_and_save(**kwargs)           # obtem os dados e persiste no lake
    date_process = get_min_date(df_new)       # data da partida mais antiga na iteração
    min_match_id = get_min_match_id(df_new)   # id da partida mais antiga da iteração

    print(min_match_id)
    while max_date <= date_process:
        df_new = get_and_save(less_than_match_id=min_match_id, **kwargs)
        date_process = get_min_date(df_new)
        min_match_id = get_min_match_id(df_new)
        print(min_match_id)

# COMMAND ----------

API_KEY = dbutils.secrets.get(scope="dota", key="api_key")
mode = dbutils.widgets.get("mode")

if mode == "new":
    get_new_pro_matches(since=-7,api_key=API_KEY)

elif mode == "history":
    get_history_pro_matches(api_key=API_KEY)
