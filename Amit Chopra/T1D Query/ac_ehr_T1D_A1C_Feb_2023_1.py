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

# MAGIC %sql
# MAGIC -- drop table if exists ac_ehr_ins_202211;
# MAGIC -- CREATE TABLE ac_ehr_ins_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/Insurance";
# MAGIC
# MAGIC -- drop table if exists ac_ehr_diag_202211;
# MAGIC -- CREATE TABLE ac_ehr_diag_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/Diagnosis";
# MAGIC
# MAGIC
# MAGIC -- drop table if exists ac_patient_202211;
# MAGIC -- CREATE TABLE ac_patient_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/Patient";
# MAGIC
# MAGIC
# MAGIC -- drop table if exists ac_labs_ehr_202211;
# MAGIC -- CREATE TABLE ac_labs_ehr_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/Lab";
# MAGIC
# MAGIC -- drop table if exists ac_Proc_ehr_202211;
# MAGIC -- CREATE TABLE ac_Proc_ehr_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/Procedure";
# MAGIC
# MAGIC drop table if exists ac_Proc_ehr_202211_test;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ac_proc_ehr_202211;
# MAGIC CREATE TABLE ac_proc_ehr_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/Procedure";
# MAGIC
# MAGIC drop table if exists ac_Rx_Presc_ehr_202211;
# MAGIC CREATE TABLE ac_Rx_Presc_ehr_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/RX Prescribed";
# MAGIC
# MAGIC drop table if exists ac_Med_Admn_ehr_202211;
# MAGIC CREATE TABLE ac_Med_Admn_ehr_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/RX Administration";
# MAGIC
# MAGIC drop table if exists ac_Patient_ehr_202211;
# MAGIC CREATE TABLE ac_Patient_ehr_202211 USING DELTA LOCATION "/mnt/optummarkt/202211/ontology/base/Patient";
# MAGIC

# COMMAND ----------

spark.table("ac_t1d_a1c_value_tst").write.format("csv").mode("overwrite").option("header","true").save("dbfs:/mnt/rwe-projects-ac/amit/T1D_A1c");

# COMMAND ----------



ac_ndc_type1_diab_list_loc="/FileStore/tables/NDC_List.csv"

# COMMAND ----------

# CSV OPTIONS
infer_schema = "false"
first_row_is_header = "true"
file_type = "csv"
delimiter = ","

# COMMAND ----------

typ1_diab_ndc_list = spark.read.format(file_type).option("inferSchema",infer_schema).option("header",first_row_is_header).option("sep",delimiter).load(ac_ndc_type1_diab_list_loc)

typ1_diab_ndc_list.write.mode("overwrite").saveAsTable("default.type1_diab_ndc_list")
