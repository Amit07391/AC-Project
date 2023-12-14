-- Databricks notebook source

drop table if exists ac_dod_2303_lab_clm;

create table ac_dod_2303_lab_clm using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Lab Results';

select * from ac_dod_2303_lab_clm;



-- COMMAND ----------

select distinct proc_cd,tst_desc from ac_dod_2303_lab_clm
where upper(tst_desc) like '%TRIGLYCERIDE%'

-- COMMAND ----------

create or replace table ac_dod_trigly_lab_1 as
select distinct * from ac_dod_2303_lab_clm
where upper(tst_desc) like '%TRIGLYCERIDE%'
;

-- COMMAND ----------

select distinct * from ac_dod_trigly_lab_1
order by patid, fst_dt

-- COMMAND ----------

select count(distinct patid) from ac_dod_trigly_lab_1
