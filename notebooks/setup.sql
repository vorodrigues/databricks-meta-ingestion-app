-- Databricks notebook source
-- MAGIC %md # Meta Ingestion - Setup

-- COMMAND ----------

-- MAGIC %md ## 1/ Setup databases

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS vr_demo_dev;
CREATE DATABASE IF NOT EXISTS vr_demo_dev.control;

-- COMMAND ----------

-- MAGIC %md ## 2/ Setup control table

-- COMMAND ----------

DROP TABLE IF EXISTS vr_demo_dev.control.meta_ingestion_config

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS vr_demo_dev.control.meta_ingestion_config (
  ingestion_group STRING NOT NULL COMMENT 'Unique ingestion group name',
  target_catalog STRING NOT NULL COMMENT 'Destination catalog',
  target_database STRING NOT NULL COMMENT 'Destination database',
  target_table STRING NOT NULL COMMENT 'Destination table',
  active BOOLEAN COMMENT 'Used for enabling/disabling ingestion for this table',
  source_type STRING COMMENT 'Source type (kafka, file, etc.)',
  source_file_path STRING COMMENT 'Source file path',
  source_file_aux_path STRING COMMENT 'Path used to store schema and checkpoint data',
  source_kafka_topic STRING COMMENT 'Source Kafka topic',
  source_kafka_schema STRING COMMENT 'Used for desearlizing data from Kafka',
  silver_transformations STRING COMMENT 'Used for defining transformations on silver tables',
  silver_time_key STRING COMMENT 'Used for sorting and deduplicating data',
  silver_merge_keys STRING COMMENT 'Used for merging data from bronze to silver tables',
  silver_clustering_keys STRING COMMENT 'Used for defining liquid clustering keys',
  time_last_modified TIMESTAMP COMMENT 'Time of last modification',
  time_last_promoted TIMESTAMP COMMENT 'Time of last promotion',
  CONSTRAINT meta_ingestion_config_pk PRIMARY KEY(ingestion_group, target_catalog, target_database, target_table)
)
PARTITIONED BY (ingestion_group)

-- COMMAND ----------

GRANT ALL PRIVILEGES ON TABLE vr_demo_dev.control.meta_ingestion_config TO `ba0e58c4-b780-4d62-8cd5-0fe0c75b396f`

-- COMMAND ----------

-- MAGIC %md ## 3/ Example table registration

-- COMMAND ----------

-- MAGIC %py
-- MAGIC values = []
-- MAGIC for group in range(1, 101):
-- MAGIC   for table in range(1, 201):
-- MAGIC     values.append(f"""
-- MAGIC       (
-- MAGIC         'group{group}',
-- MAGIC         'vr_demo',
-- MAGIC         'group{group}',
-- MAGIC         'table{table}',
-- MAGIC         true,
-- MAGIC         'file',
-- MAGIC         's3://src-bucket/group{group}/table{table}',
-- MAGIC         's3://dst-bucket/control/group{group}/table{table}',
-- MAGIC         '',
-- MAGIC         '',
-- MAGIC         '* EXCEPT (id)',
-- MAGIC         'eventid',
-- MAGIC         'processingTime',
-- MAGIC         'time,action',
-- MAGIC         null,
-- MAGIC         null
-- MAGIC       )
-- MAGIC     """)
-- MAGIC values = ",".join(values)
-- MAGIC
-- MAGIC result = spark.sql(f"INSERT INTO vr_demo_dev.control.meta_ingestion_config VALUES {values}")
-- MAGIC display(result)
