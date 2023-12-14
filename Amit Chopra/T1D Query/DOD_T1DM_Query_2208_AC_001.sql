-- Databricks notebook source
select distinct * from ty00_ses_rx_anti_dm_loopup

-- COMMAND ----------

----------Import SES Member Continuous Enrollment records---------
drop table if exists ac_dod_2208_mem_conti;

create table ac_dod_2208_mem_conti using delta location 'dbfs:/mnt/optumclin/202208/ontology/base/dod/Member Continuous Enrollment';

select * from ac_dod_2208_mem_conti;


-- COMMAND ----------

-- MAGIC %md #### Filtering for rapid acting insulins only

-- COMMAND ----------

drop table ac_rx_anti_dm_1;
create or replace table ac_rx_anti_RAI_dm_1 as
select distinct * from ty37_rx_anti_dm
where NDC in ('00002751001',	'00002751017',	'00002751099',	'00002751559',	'00002751601',	'00002751659',	'00002751699',	'00002872501',	'00002872559',	'00002872599',	'00074751001',	'00088250000',	'00088250001',	'00088250033',	'00088250034',	'00088250052',	'00088250201',	'00088250205',	'00110530401',	'00169330311',	'00169330312',	'00169330390',	'00169330391',	'00169633800',	'00169633810',	'00169633897',	'00169633898',	'00169633910',	'00169633997',	'00169633998',	'00169750111',	'00169750112',	'00169750190',	'00409256710',	'00420330312',	'00420330390',	'00420330391',	'00420633910',	'00420633990',	'00420750111',	'00420750190',	'12854033700',	'12854033701',	'12854033733',	'12854033734',	'12854033752',	'12854033805',	'35356010200',	'50090137500',	'50090166400',	'50090166500',	'50090167800',	'54569643500',	'54569658400',	'54569658500',	'54569658600',	'54569658700',	'54868277700',	'54868510800',	'54868583600',	'54868589900',	'54868605400',	'55045360201',	'62381751509',	'62381751601',	'62381751605',	'62381751609',	'62381872505',	'62381872509',	'64725075001',	'66143751005',	'68115074610',	'68258889903',	'68258892803',	'68258896701');

-- COMMAND ----------

select distinct * from ty37_rx_anti_dm
where AHFSCLSS_DESC like '%INSULIN%' and rx_type='Inhaler Insulin'

-- COMMAND ----------


create or replace table ac_rx_anti_dm_1 as
select distinct * from ty37_rx_anti_dm
where rx_type in ('Basal','Bolus','PreMix');

select distinct * from ac_rx_anti_dm_1
order by patid, fill_dt;

-- COMMAND ----------

select distinct * from ac_rx_anti_RAI_dm_1
where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31'
order by patid, fill_dt;

-- select distinct strength from ac_rx_anti_RAI_dm_1
-- where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31'


-- COMMAND ----------

-- MAGIC %md #### Average Dose

-- COMMAND ----------

create or replace table ac_rx_anti_RAI_dm_Dose_1 as
select distinct patid, clmid, fill_dt, sum(days_sup) as Days_suply, sum(quantity) as Qty from ac_rx_anti_dm_1
where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31'
group by 1,2,3
order by patid, fill_dt;

select distinct * from ac_rx_anti_RAI_dm_Dose_1
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_rx_anti_RAI_dm_Dose_1 as
select distinct patid, clmid, fill_dt, sum(days_sup) as Days_suply, sum(quantity) as Qty from ac_rx_anti_RAI_dm_1
where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31'
group by 1,2,3
order by patid, fill_dt;

select distinct * from ac_rx_anti_RAI_dm_Dose_1
order by patid, fill_dt;

-- COMMAND ----------

select distinct patid, count(distinct fill_dt) from ac_rx_anti_RAI_dm_Dose_1
group by 1
order by 2 desc;

-- COMMAND ----------

create or replace table ac_rx_anti_RAI_dm_Dose_2 as
select distinct *, (100*Qty)/Days_suply as Dose from ac_rx_anti_RAI_dm_Dose_1
order by patid, fill_dt;

select distinct * from ac_rx_anti_RAI_dm_Dose_2
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_rx_anti_RAI_dm_Dose_3 as
select distinct patid, sum(dose) as Dose_per_patient from ac_rx_anti_RAI_dm_Dose_2
group by 1
order by 1;

select distinct * from ac_rx_anti_RAI_dm_Dose_3
;

-- COMMAND ----------

select avg(Dose_per_patient) from ac_rx_anti_RAI_dm_Dose_3;

select percentile_approx(Dose_per_patient, 0.5) as median, median(Dose_per_patient)  as md from ac_rx_anti_RAI_dm_Dose_3;
