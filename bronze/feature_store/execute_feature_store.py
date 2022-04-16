# Databricks notebook source
from delta import *

# COMMAND ----------

def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

# COMMAND ----------

TABLE_NAME = 'bronze_dota.tb_feature_store_player_match'
DATE_REF = dbutils.widgets.get("dt_ref")

delta_table = DeltaTable.forName(spark,TABLE_NAME)

# COMMAND ----------

query = import_query("etl_player_match.sql")
query = query.format(date=DATE_REF)
df = spark.sql(query)

# COMMAND ----------

(delta_table.alias("d")
            .merge(df.alias("n"), "d.account_id = n.account_id and d.dt_ref = n.dt_ref")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

delta_table.vacuum()
