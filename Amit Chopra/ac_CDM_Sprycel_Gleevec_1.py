# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC from pyspark.sql.types import StringType
# MAGIC from pyspark.sql.types import IntegerType
# MAGIC
# MAGIC from pyspark.sql.functions import expr
# MAGIC from pyspark.sql.functions import lit, when, concat, trim, col, desc, to_date, datediff, year,to_csv,date_add
# MAGIC from pyspark.sql.functions import countDistinct
# MAGIC
# MAGIC from delta.tables import *

# COMMAND ----------

# MAGIC %fs ls "/mnt/optumclin/202210/ontology/base/dod";

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ac_cdm_diag_202210;
# MAGIC CREATE TABLE ac_cdm_diag_202210 USING DELTA LOCATION "/mnt/optumclin/202210/ontology/base/dod/Medical Diagnosis";
# MAGIC
# MAGIC drop table if exists ac_cdm_med_claims_202210;
# MAGIC CREATE TABLE ac_cdm_med_claims_202210 USING DELTA LOCATION "/mnt/optumclin/202210/ontology/base/dod/Medical Claims";
# MAGIC
# MAGIC drop table if exists ac_cdm_mem_cont_enrol_202210;
# MAGIC CREATE TABLE ac_cdm_mem_cont_enrol_202210 USING DELTA LOCATION "/mnt/optumclin/202210/ontology/base/dod/Member Continuous Enrollment";
# MAGIC
# MAGIC drop table if exists ac_cdm_RX_claims_202210;
# MAGIC CREATE TABLE ac_cdm_RX_claims_202210 USING DELTA LOCATION "/mnt/optumclin/202210/ontology/base/dod/RX Claims";
# MAGIC
# MAGIC
