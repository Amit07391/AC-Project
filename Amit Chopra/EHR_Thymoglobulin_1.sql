-- Databricks notebook source
-- drop table if exists ac_ehr_RX_Presc_202303;
-- CREATE TABLE ac_ehr_RX_Presc_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/RX Prescribed";

-- select distinct * from ac_ehr_RX_Presc_202303;

-- select max(rxdate) from ac_ehr_RX_Presc_202303;

-- drop table if exists ac_ehr_Med_Admn_202303;
-- CREATE TABLE ac_ehr_Med_Admn_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/RX Administration";

-- select distinct * from ac_ehr_Med_Admn_202303;


-- drop table if exists ac_ehr_proc_202303;
-- CREATE TABLE ac_ehr_proc_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Procedure";

-- select distinct * from ac_ehr_proc_202303;

drop table if exists ac_ehr_enctr_202303;
CREATE TABLE ac_ehr_enctr_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Encounter";

select distinct * from ac_ehr_enctr_202303;

-- COMMAND ----------

create or replace table ac_ehr_thymo_rx_presc as
select distinct * from ac_ehr_RX_Presc_202303
where NDC in ('58468008001','62053053425') or upper(DRUG_NAME) like '%RABBIT%' or upper(GENERIC_DESC) like '%RABBIT%'
order by ptid, rxdate;

select distinct * from ac_ehr_thymo_rx_presc
where upper(drug_name) like '%RABBIT%' or upper(GENERIC_DESC) like '%RABBIT%'
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_ehr_thymo_med_admin as
select distinct * from ac_ehr_Med_Admn_202303
where NDC in ('58468008001','62053053425') or upper(DRUG_NAME) like '%THYMO%' or upper(GENERIC_DESC) like '%THYMO%'
order by ptid, admin_date;

select distinct * from ac_ehr_thymo_med_admin
where upper(drug_name) like '%RABBIT%' or upper(GENERIC_DESC) like '%RABBIT%'
order by ptid, admin_date;

-- COMMAND ----------

create or replace table ac_ehr_thymo_med_admin_1 as 
select distinct a.*, b.interaction_type, b.interaction_date from ac_ehr_thymo_med_admin a
left join ac_ehr_enctr_202303 b on a.ptid=b.ptid and a.encid=b.encid
order by ptid, coalesce(admin_date, order_date);

-- COMMAND ----------

select distinct * from ac_ehr_thymo_med_admin_1
where upper(drug_name) like '%RABBIT%' or upper(GENERIC_DESC) like '%RABBIT%'
order by ptid, coalesce(admin_date, order_date);

-- COMMAND ----------

create or replace table ac_ehr_thymo_proc as
select distinct a.*, b.interaction_type, b.interaction_date from ac_ehr_proc_202303 a
left join ac_ehr_enctr_202303 b on a.ptid=b.ptid and a.encid=b.encid
where proc_code in ('J7511','J7504')
order by ptid, proc_date;

-- COMMAND ----------

select distinct * from ac_ehr_thymo_proc
order by ptid, proc_date;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC
-- MAGIC from pyspark.sql.functions import expr
-- MAGIC from pyspark.sql.functions import lit, when, concat, trim, col, desc, to_date, datediff, year,to_csv,date_add
-- MAGIC from pyspark.sql.functions import countDistinct
-- MAGIC
-- MAGIC from delta.tables import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("ac_ehr_thymo_proc").write.format("csv").mode("overwrite").option("header","true").save("dbfs:/mnt/rwe-projects-ac/amit");
