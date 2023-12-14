-- Databricks notebook source
-- MAGIC %md #### Checking triglycerides and LDL lab values

-- COMMAND ----------

select distinct TEST_NAME from ac_ehr_lab_202303
where 
upper(test_name) like "%TRIGLYCERIDES%"  ;
-- upper(test_name) like "%LDL%" or lower(test_name) like "%low%" or lower(test_name) like "%lipoprotein%" or 
--Cholesterol.LDL

-- COMMAND ----------

create or replace table ac_ehr_act_panc_lab_1 as
select distinct a.*, coalesce(a.result_date,a.collected_date,a.order_date) as service_date, b.indx_dt, b.yr_dt from ac_ehr_lab_202303 a 
inner join ac_ehr_acute_panc_proc_2 b on a.PTID=b.ptid
where test_name='Cholesterol.LDL' or upper(test_name) like "%TRIGLYCERIDES%" ;
select distinct * from ac_ehr_act_panc_lab_1
order by ptid, service_date;

-- COMMAND ----------

select distinct test_result, RESULT_UNIT from ac_ehr_act_panc_lab_1
where upper(test_name) like "%TRIGLYCERIDES%";

-- COMMAND ----------

-- MAGIC %md ####CHecking tryglycerides

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_TG_1 as
select distinct * from ac_ehr_act_panc_lab_1
where upper(test_name) like "%TRIGLYCERIDES%" and service_date between indx_dt - 366 and indx_dt
order by ptid, service_date;

select distinct * from ac_ehr_acute_panc_lab_TG_1
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_TG_max_dt as
select distinct ptid, max(concat(service_date," ",coalesce(result_time,collected_time,order_time))) as max_dt from ac_ehr_acute_panc_lab_TG_1
group by 1
order by 1;

select distinct * from ac_ehr_acute_panc_lab_TG_max_dt
order by 1;


-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_TG_2 as
select distinct a.*, cast(test_result as FLOAT) as result_val from  ac_ehr_acute_panc_lab_TG_1 a
inner join ac_ehr_acute_panc_lab_TG_max_dt b on a.ptid=b.ptid and concat(a.service_date," ",coalesce(a.result_time,a.collected_time,a.order_time))=b.max_dt
order by ptid, service_date;


select distinct * from ac_ehr_acute_panc_lab_TG_2
-- where obs_date='2007-02-20'
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_TG_3 as
select distinct yr_dt,ptid, avg(result_val) as avg from ac_ehr_acute_panc_lab_TG_2
group by 1,2
order by 1,2;

select distinct * from ac_ehr_acute_panc_lab_TG_3
order by 1,2;


-- COMMAND ----------

select yr_dt, count(distinct ptid) from ac_ehr_acute_panc_lab_TG_2
group by 1
order by 1;

-- yr_dt	count(DISTINCT ptid)
-- 2017	1347
-- 2018	1170
-- 2019	1135
-- 2020	822
-- 2021	705
-- 2022	248

-- COMMAND ----------

select yr_dt, min(avg) as min, max(avg) as max, mean(avg) as mean from ac_ehr_acute_panc_lab_TG_3
group by 1
order by 1;      

select yr_dt, count(distinct ptid) as pts from ac_ehr_acute_panc_lab_TG_3
where avg>900
group by 1
order by 1;

select yr_dt, count(distinct ptid) as pts from ac_ehr_acute_panc_lab_TG_3
where avg>1800
group by 1
order by 1;   



-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_TG_4 as
select distinct *, avg*0.0556 as Val_mmol from ac_ehr_acute_panc_lab_TG_3
order by 1,2;

select distinct *  from ac_ehr_acute_panc_lab_TG_4
order by 1,2;  

-- COMMAND ----------

select yr_dt, count(distinct ptid) as pts from ac_ehr_acute_panc_lab_TG_4
where Val_mmol>1700
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Checking LDL C levels

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_LDL_1 as
select distinct * from ac_ehr_act_panc_lab_1
where test_name='Cholesterol.LDL' and service_date between indx_dt - 366 and indx_dt
order by ptid, service_date;

select distinct * from ac_ehr_acute_panc_lab_LDL_1
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_LDL_max_dt as
select distinct ptid, max(concat(service_date," ",coalesce(result_time,collected_time,order_time))) as max_dt from ac_ehr_acute_panc_lab_LDL_1
group by 1
order by 1;

select distinct * from ac_ehr_acute_panc_lab_LDL_max_dt
order by 1;


-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_LDL_2 as
select distinct a.*, cast(test_result as FLOAT) as result_val from  ac_ehr_acute_panc_lab_LDL_1 a
inner join ac_ehr_acute_panc_lab_LDL_max_dt b on a.ptid=b.ptid and concat(a.service_date," ",coalesce(a.result_time,a.collected_time,a.order_time))=b.max_dt
order by ptid, service_date;


select distinct * from ac_ehr_acute_panc_lab_LDL_2
-- where obs_date='2007-02-20'
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_lab_LDL_3 as
select distinct yr_dt,ptid, avg(result_val) as avg from ac_ehr_acute_panc_lab_LDL_2
group by 1,2
order by 1,2;

select distinct * from ac_ehr_acute_panc_lab_LDL_3
order by 1,2;


-- COMMAND ----------

select yr_dt, count(distinct ptid) from ac_ehr_acute_panc_lab_LDL_3
group by 1
order by 1;

-- select distinct result_val, result_unit from ac_ehr_acute_panc_lab_LDL_2;

-- COMMAND ----------

select yr_dt, min(avg) as min, max(avg) as max, mean(avg) as mean from ac_ehr_acute_panc_lab_LDL_3
group by 1
order by 1;      

-- select yr_dt, count(distinct ptid) as pts from ac_ehr_acute_panc_lab_LDL_3
-- where avg>900
-- group by 1
-- order by 1;

-- select yr_dt, count(distinct ptid) as pts from ac_ehr_acute_panc_lab_LDL_3
-- where avg>1800
-- group by 1
-- order by 1;   


