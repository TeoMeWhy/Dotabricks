# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import datetime

from delta import *

from tqdm import tqdm

# COMMAND ----------

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def create_date_list(start:str, stop:str)->list:
    date_start = datetime.datetime.strptime(start, "%Y-%m-%d")
    date_stop = datetime.datetime.strptime(stop, "%Y-%m-%d")
    days = (date_stop - date_start).days
    dates = [(date_start + datetime.timedelta(i)).strftime("%Y-%m-%d") for i in range(days+1)]
    return dates

def upsert(query, delta_table, date):
    
    query = query.format(date=date)
    df = spark.sql(query)
    
    (delta_table.alias("d")
                .merge(df.alias("n"), "d.account_id = n.account_id and d.dt_ref = n.dt_ref")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

    delta_table.vacuum()
    
def backfill(date_start, date_stop):
    
    delta_table = DeltaTable.forName(spark,TABLE_NAME)
    query = import_query("etl_player_match.sql")
    dates = create_date_list(date_start, date_stop)
    
    for d in tqdm(dates):
        upsert(query, delta_table, d)

# COMMAND ----------

TABLE_NAME = 'bronze_dota.tb_feature_store_player_match'
DATE_START = dbutils.widgets.get("dt_start")
DATE_STOP = dbutils.widgets.get("dt_stop")

# COMMAND ----------

backfill(DATE_START, DATE_STOP)
