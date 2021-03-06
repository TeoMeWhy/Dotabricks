-- Databricks notebook source
DROP TABLE IF EXISTS `bronze_dota`.`tb_feature_store_player_match`;
CREATE TABLE `bronze_dota`.`tb_feature_store_player_match` (
  `dt_ref` STRING,
  `dt_atualizacao` TIMESTAMP,
  `account_id` BIGINT,
  `recencia` INT,
  `qtde_dias_game` BIGINT,
  `qtde_matches` BIGINT,
  `avg_camps_stacked` DOUBLE,
  `avg_creeps_stacked` DOUBLE,
  `avg_deaths` DOUBLE,
  `avg_denies` DOUBLE,
  `avg_firstblood_claimed` DOUBLE,
  `avg_gold` DOUBLE,
  `avg_gold_per_min` DOUBLE,
  `avg_gold_spent` DOUBLE,
  `avg_hero_damage` DOUBLE,
  `avg_hero_healing` DOUBLE,
  `avg_kill_streaks_1` DOUBLE,
  `avg_kill_streaks_2` DOUBLE,
  `avg_kill_streaks_3` DOUBLE,
  `avg_kill_streaks_4` DOUBLE,
  `avg_kill_streaks_5` DOUBLE,
  `avg_kills` DOUBLE,
  `avg_last_hits` DOUBLE,
  `avg_level` DOUBLE,
  `avg_net_worth` DOUBLE,
  `avg_roshans_killed` DOUBLE,
  `avg_rune_pickups` DOUBLE,
  `avg_stuns` DOUBLE,
  `avg_teamfight_participation` DOUBLE,
  `avg_tower_damage` DOUBLE,
  `avg_towers_killed` DOUBLE,
  `avg_xp_per_min` DOUBLE,
  `avg_duration` DOUBLE,
  `avg_isRadiant` DOUBLE,
  `avg_win` DOUBLE,
  `avg_total_gold` DOUBLE,
  `avg_total_xp` DOUBLE,
  `avg_kills_per_min` DOUBLE,
  `avg_kda` DOUBLE,
  `avg_abandons` DOUBLE,
  `avg_neutral_kills` DOUBLE,
  `avg_tower_kills` DOUBLE,
  `avg_courier_kills` DOUBLE,
  `avg_lane_kills` DOUBLE,
  `avg_hero_kills` DOUBLE,
  `avg_observer_kills` DOUBLE,
  `avg_sentry_kills` DOUBLE,
  `avg_roshan_kills` DOUBLE,
  `avg_necronomicon_kills` DOUBLE,
  `avg_ancient_kills` DOUBLE,
  `avg_buyback_count` DOUBLE,
  `avg_observer_uses` DOUBLE,
  `avg_sentry_uses` DOUBLE,
  `avg_lane_efficiency_pct` DOUBLE,
  `avg_lane` DOUBLE,
  `avg_lane_role` DOUBLE,
  `avg_purchase_tpscroll` DOUBLE,
  `avg_actions_per_min` DOUBLE,
  `avg_rank_tier` DOUBLE,
  `top01_hero` BIGINT,
  `top02_hero` BIGINT,
  `top03_hero` BIGINT,
  `top04_hero` BIGINT,
  `top05_hero` BIGINT,
  `top06_hero` BIGINT,
  `top07_hero` BIGINT,
  `top08_hero` BIGINT,
  `top09_hero` BIGINT,
  `top10_hero` BIGINT,
  `top01_gold_per_min` DOUBLE,
  `top02_gold_per_min` DOUBLE,
  `top03_gold_per_min` DOUBLE,
  `top04_gold_per_min` DOUBLE,
  `top05_gold_per_min` DOUBLE,
  `top06_gold_per_min` DOUBLE,
  `top07_gold_per_min` DOUBLE,
  `top08_gold_per_min` DOUBLE,
  `top09_gold_per_min` DOUBLE,
  `top10_gold_per_min` DOUBLE,
  `top01_last_hits` DOUBLE,
  `top02_last_hits` DOUBLE,
  `top03_last_hits` DOUBLE,
  `top04_last_hits` DOUBLE,
  `top05_last_hits` DOUBLE,
  `top06_last_hits` DOUBLE,
  `top07_last_hits` DOUBLE,
  `top08_last_hits` DOUBLE,
  `top09_last_hits` DOUBLE,
  `top10_last_hits` DOUBLE,
  `top01_avg_denies` DOUBLE,
  `top02_avg_denies` DOUBLE,
  `top03_avg_denies` DOUBLE,
  `top04_avg_denies` DOUBLE,
  `top05_avg_denies` DOUBLE,
  `top06_avg_denies` DOUBLE,
  `top07_avg_denies` DOUBLE,
  `top08_avg_denies` DOUBLE,
  `top09_avg_denies` DOUBLE,
  `top10_avg_denies` DOUBLE,
  `top01_firstblood_claimed` DOUBLE,
  `top02_firstblood_claimed` DOUBLE,
  `top03_firstblood_claimed` DOUBLE,
  `top04_firstblood_claimed` DOUBLE,
  `top05_firstblood_claimed` DOUBLE,
  `top06_firstblood_claimed` DOUBLE,
  `top07_firstblood_claimed` DOUBLE,
  `top08_firstblood_claimed` DOUBLE,
  `top09_firstblood_claimed` DOUBLE,
  `top10_firstblood_claimed` DOUBLE,
  `top01_xp_per_min` DOUBLE,
  `top02_xp_per_min` DOUBLE,
  `top03_xp_per_min` DOUBLE,
  `top04_xp_per_min` DOUBLE,
  `top05_xp_per_min` DOUBLE,
  `top06_xp_per_min` DOUBLE,
  `top07_xp_per_min` DOUBLE,
  `top08_xp_per_min` DOUBLE,
  `top09_xp_per_min` DOUBLE,
  `top10_xp_per_min` DOUBLE,
  `top01_win_rate` DOUBLE,
  `top02_win_rate` DOUBLE,
  `top03_win_rate` DOUBLE,
  `top04_win_rate` DOUBLE,
  `top05_win_rate` DOUBLE,
  `top06_win_rate` DOUBLE,
  `top07_win_rate` DOUBLE,
  `top08_win_rate` DOUBLE,
  `top09_win_rate` DOUBLE,
  `top10_win_rate` DOUBLE,
  `top01_kda` DOUBLE,
  `top02_kda` DOUBLE,
  `top03_kda` DOUBLE,
  `top04_kda` DOUBLE,
  `top05_kda` DOUBLE,
  `top06_kda` DOUBLE,
  `top07_kda` DOUBLE,
  `top08_kda` DOUBLE,
  `top09_kda` DOUBLE,
  `top10_kda` DOUBLE,
  `top01_observer_uses` DOUBLE,
  `top02_observer_uses` DOUBLE,
  `top03_observer_uses` DOUBLE,
  `top04_observer_uses` DOUBLE,
  `top05_observer_uses` DOUBLE,
  `top06_observer_uses` DOUBLE,
  `top07_observer_uses` DOUBLE,
  `top08_observer_uses` DOUBLE,
  `top09_observer_uses` DOUBLE,
  `top10_observer_uses` DOUBLE,
  `top01_sentry_uses` DOUBLE,
  `top02_sentry_uses` DOUBLE,
  `top03_sentry_uses` DOUBLE,
  `top04_sentry_uses` DOUBLE,
  `top05_sentry_uses` DOUBLE,
  `top06_sentry_uses` DOUBLE,
  `top07_sentry_uses` DOUBLE,
  `top08_sentry_uses` DOUBLE,
  `top09_sentry_uses` DOUBLE,
  `top10_sentry_uses` DOUBLE)
USING delta
PARTITIONED BY (`dt_ref`);
