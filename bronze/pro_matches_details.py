# Databricks notebook source
import requests
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import window
from delta.tables import *

# COMMAND ----------

dbutils.fs.rm(BRONZE_FILES, True)
dbutils.fs.rm(CHECKPOINT_FILES, True)

# COMMAND ----------

TABLE_NAME = dbutils.widgets.get("table")
print(TABLE_NAME)

RAW_FILES = f"/mnt/datalake/raw/{TABLE_NAME}"
BRONZE_FILES = f"/mnt/datalake/bronze/dota/{TABLE_NAME}"
CHECKPOINT_FILES = f"/mnt/datalake/bronze/dota/{TABLE_NAME}_checkpoint"

ID_FIELDS_DICT = {"tb_pro_matches": ["match_id"],
                  "tb_pro_matches_players": ["match_id", "account_id"]
                 }

DATE_FIELD_DICT = {"tb_pro_matches": "start_time",
                   "tb_pro_matches_players":"start_time"}

ID_FIELDS = ID_FIELDS_DICT[TABLE_NAME]
DATE_FIELD = DATE_FIELD_DICT[TABLE_NAME]

# COMMAND ----------

check_table = TABLE_NAME in [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/bronze/dota")]
if not check_table:
    print("Realizando a primeira carga...")
    df = spark.read.parquet(RAW_FILES)
    
    windowSpec  = window.Window.partitionBy(*ID_FIELDS).orderBy(DATE_FIELD)
    df_new = ( df.withColumn("rn",F.row_number().over(windowSpec))
                 .filter("rn = 1")
                 .drop("rn")
             )
    
    df_new.write.format("delta").mode("overwrite").saveAsTable(f"bronze_dota.{TABLE_NAME}")
    
else:
    print("A primeira carga já existe..")
    
print("ok")

# COMMAND ----------

tb_pro_matches_schema = StructType( [
    StructField('match_id',LongType(), True),
    StructField('barracks_status_dire',LongType(), True),
    StructField('barracks_status_radiant',LongType(), True),
    StructField('cluster',LongType(), True),
    StructField('dire_score',LongType(), True),
    StructField('dire_team_id',LongType(), True),
    StructField('duration',LongType(), True),
    StructField('engine',LongType(), True),
    StructField('first_blood_time',LongType(), True),
    StructField('game_mode',LongType(), True),
    StructField('human_players',LongType(), True),
    StructField('leagueid',LongType(), True),
    StructField('lobby_type',LongType(), True),
    StructField('match_seq_num',LongType(), True),
    StructField('negative_votes',LongType(), True),
    StructField('positive_votes',LongType(), True),
    StructField('radiant_score',LongType(), True),
    StructField('radiant_team_id',LongType(), True),
    StructField('radiant_win',BooleanType(), True),
    StructField('start_time',LongType(), True),
    StructField('tower_status_dire',LongType(), True),
    StructField('tower_status_radiant',LongType(), True),
    StructField('version',LongType(), True),
    StructField('replay_salt',LongType(), True),
    StructField('series_id',LongType(), True),
    StructField('series_type',LongType(), True),
    StructField('league', StructType([
         StructField('leagueid',LongType(),True),
         StructField('ticket',StringType(),True),
         StructField('banner',StringType(),True),
         StructField('tier',StringType(),True),
         StructField('name',StringType(),True)]),True),
    StructField('radiant_team', StructType([
          StructField('team_id',LongType(),True),
          StructField('name',StringType(),True),
          StructField('tag',StringType(),True),
          StructField('logo_url',StringType(),True)]),True),
    StructField('dire_team', StructType([
          StructField('team_id',LongType(),True),
          StructField('name',StringType(),True),
          StructField('tag',StringType(),True),
          StructField('logo_url',StringType(),True)]),True),
    StructField('patch',LongType(), True),
    StructField('region',LongType(), True),
    StructField('throw',LongType(), True),
    StructField('loss',LongType(), True),
    StructField('replay_url', StringType(), True),])

tb_pro_matches_players_schema = StructType([
    StructField("match_id", LongType(), True),
    StructField("player_slot", LongType(), True),
    StructField("account_id", LongType(), True),
    StructField("assists", LongType(), True),
    StructField("backpack_0", LongType(), True),
    StructField("backpack_1", LongType(), True),
    StructField("backpack_2", LongType(), True),
    StructField("backpack_3", LongType(), True),
    StructField("camps_stacked", LongType(), True),
    StructField("creeps_stacked", LongType(), True),
    StructField("deaths", LongType(), True),
    StructField("denies", LongType(), True),
    StructField("firstblood_claimed", LongType(), True),
    StructField("gold", LongType(), True),
    StructField("gold_per_min", LongType(), True),
    StructField("gold_spent", LongType(), True),
    StructField("hero_damage", LongType(), True),
    StructField("hero_healing", LongType(), True),
    StructField("hero_id", LongType(), True),
    StructField("item_0", LongType(), True),
    StructField("item_1", LongType(), True),
    StructField("item_2", LongType(), True),
    StructField("item_3", LongType(), True),
    StructField("item_4", LongType(), True),
    StructField("item_5", LongType(), True),
    StructField("item_neutral", LongType(), True),
    StructField("kill_streaks", StructType([
        StructField("1", LongType(), True),
        StructField("2", LongType(), True),
        StructField("3", LongType(), True),
        StructField("4", LongType(), True),
        StructField("5", LongType(), True),
        ]),True),
    StructField("kills", LongType(),True),
    StructField("last_hits", LongType(),True),
    StructField("leaver_status", LongType(),True),
    StructField("level", LongType(),True),
    StructField("net_worth", LongType(), True),
    StructField("obs_placed", LongType(), True),
    StructField("party_id", LongType(), True),
    StructField("party_size", LongType(), True),
    StructField("pings", LongType(), True),
    StructField("pred_vict", BooleanType(), True),
    StructField("randomed", BooleanType(), True),
    StructField("roshans_killed", LongType(), True),
    StructField("rune_pickups", LongType(), True),
    StructField("stuns", StringType(), True),
    StructField("teamfight_participation", StringType(), True),
    StructField("tower_damage", LongType(), True),
    StructField("towers_killed", LongType(), True),
    StructField("xp_per_min", LongType(), True),
    StructField("personaname", StringType(), True),
    StructField("name", StringType(), True),
    StructField("radiant_win", BooleanType(),True),
    StructField("start_time", LongType(), True),
    StructField("duration", LongType(), True),
    StructField("cluster", LongType(), True),
    StructField("lobby_type", LongType(), True),
    StructField("game_mode", LongType(), True),
    StructField("is_contributor", BooleanType(),True),
    StructField("patch", LongType(), True),
    StructField("region", LongType(), True),
    StructField("isRadiant", BooleanType(),True),
    StructField("win", LongType(), True),
    StructField("lose", LongType(), True),
    StructField("total_gold", LongType(), True),
    StructField("total_xp", LongType(), True),
    StructField("kills_per_min", DoubleType(),True),
    StructField("kda", LongType(), True),
    StructField("abandons", LongType(), True),
    StructField("neutral_kills", LongType(), True),
    StructField("tower_kills", LongType(), True),
    StructField("courier_kills", LongType(), True),
    StructField("lane_kills", LongType(), True),
    StructField("hero_kills", LongType(), True),
    StructField("observer_kills", LongType(), True),
    StructField("sentry_kills", LongType(), True),
    StructField("roshan_kills", LongType(), True),
    StructField("necronomicon_kills", LongType(), True),
    StructField("ancient_kills", LongType(), True),
    StructField("buyback_count", LongType(), True),
    StructField("observer_uses", LongType(), True),
    StructField("sentry_uses", LongType(), True),
    StructField("lane_efficiency",DoubleType(),True),
    StructField("lane_efficiency_pct", LongType(), True),
    StructField("lane", LongType(), True),
    StructField("lane_role", LongType(), True),
    StructField("is_roaming", BooleanType(),True),
    StructField("purchase_tpscroll", LongType(), True),
    StructField("actions_per_min", LongType(), True),
    StructField("life_state_dead", LongType(), True),
    StructField("rank_tier", LongType(), True),])

schemas = {"tb_pro_matches": tb_pro_matches_schema,
           "tb_pro_matches_players": tb_pro_matches_players_schema,
          }

stream_schema = schemas[TABLE_NAME]

# COMMAND ----------

bronzeDeltaTable = DeltaTable.forPath(spark, BRONZE_FILES)

def upsertToDelta(df, batchId):
    windowSpec  = window.Window.partitionBy(*ID_FIELDS).orderBy(DATE_FIELD)
    df_new = ( df.withColumn("rn",F.row_number().over(windowSpec))
                 .filter("rn = 1")
                 .drop("rn"))
    
    join = " and ".join([f"delta.{i} = raw.{i}" for i in ID_FIELDS])
    ( bronzeDeltaTable.alias("delta") 
                      .merge(df_new.alias("raw"), join) 
                      .whenMatchedUpdateAll()
                      .whenNotMatchedInsertAll()
                      .execute() )
    
df_stream = ( spark.readStream
                   .format('cloudFiles')
                   .option('cloudFiles.format', 'parquet')
                   .schema(stream_schema)
                   .load(RAW_FILES))

stream = (df_stream.writeStream
                   .foreachBatch(upsertToDelta)
                   .option('checkpointLocation', CHECKPOINT_FILES)
                   .outputMode("update")
                   .start() 
         )

# COMMAND ----------

stream.processAllAvailable()
stream.stop()

bronzeDeltaTable.vacuum()
