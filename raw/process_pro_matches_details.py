# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import requests
from pyspark.sql.types import *
from pyspark.sql import functions as F
import json
from tqdm import tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tabela match

# COMMAND ----------

MATCH_SCHEMA = StructType( [StructField('match_id',LongType(), True),
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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tabela Match Players

# COMMAND ----------

MATCH_PLAYERS_SCHEMA = StructType([StructField("match_id", LongType(), True),
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

# COMMAND ----------

def get_data(match_id):
    path = f"/dbfs/mnt/datalake/raw/pro_matches_landing/{match_id}.json"
    return json.load(open(path,"r"))

def save_data(df, tb_name):
    ( df.coalesce(1)
        .write
        .mode("append")
        .format("parquet")
        .save(f"/mnt/datalake/raw/{tb_name}"))

def save_match_df(data):
    df = spark.createDataFrame([data], schema=MATCH_SCHEMA)
    save_data(df, "tb_pro_matches")
    
def save_match_chat_df(data):
      
    if data["chat"] != None and len(data["chat"]) > 0:
        df = spark.createDataFrame(data["chat"])
        df = df.withColumn("match_id", F.lit(data["match_id"]) )
        save_data(df, "tb_pro_match_chat")

def save_match_picks_bans(data):
    if data["picks_bans"] != None:
        df = spark.createDataFrame(data["picks_bans"])
        save_data(df, "tb_pro_match_picks_bans")

def save_radiant_gold_adv_df(data):
    if data["radiant_gold_adv"] != None:
        data_new = [ {"value": i, "match_id":data["match_id"]} for i in data["radiant_gold_adv"] ]
        df = spark.createDataFrame(data_new)
        save_data(df, "tb_pro_match_radiant_gold_adv")

def save_radiant_xp_adv_df(data):
    if data["radiant_xp_adv"] != None:
        data_new = [ {"value": i, "match_id":data["match_id"]} for i in data["radiant_xp_adv"] ]
        df = spark.createDataFrame(data_new)
        save_data(df, "tb_pro_match_radiant_xp_adv")

def save_match_player_df(data):
    df = spark.createDataFrame(data["players"], schema=MATCH_PLAYERS_SCHEMA)
    save_data(df, "tb_pro_matches_players")

def process_match(match_id):
    
    data = get_data(match_id)

    save_match_chat_df(data)
    save_match_picks_bans(data)
    save_radiant_gold_adv_df(data)
    save_radiant_xp_adv_df(data)
    save_match_player_df(data)
    save_match_df(data)
    
def get_ids():
    df_proceeded = spark.read.format("delta").load("/mnt/datalake/raw/pro_matches_proceeded/")
    
    if "tb_pro_matches" in [i.name.strip("/") for i in dbutils.fs.ls("/mnt/datalake/raw/")]:
        df_pro_matches = spark.read.parquet("/mnt/datalake/raw/tb_pro_matches/")

        df_proceeded = (df_proceeded.join(df_pro_matches, "match_id", "left")
                                    .filter("duration is null"))

    return df_proceeded.select("match_id")

# COMMAND ----------

match_ids = get_ids()
for i in tqdm(match_ids.collect()):
    process_match(i[0])
