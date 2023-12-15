-- Databricks notebook source
-- MAGIC %md #### Checking prior acute pancreatitis hospitalization

-- COMMAND ----------

drop table if exists ac_ehr_dx_acute_panc_pre_indx;
create or replace table ac_ehr_dx_acute_panc_pre_indx as
select distinct a.ptid, a.encid, diag_date, a.DIAGNOSIS_CD, a.DIAGNOSIS_STATUS, a.DIAGNOSIS_CD_TYPE, c.yr_dt, c.indx_dt
from ac_ehr_diag_202303 a
inner join ac_ehr_acute_panc_proc_2 c on a.ptid=c.ptid 
where (a.DIAGNOSIS_CD='5770' or a.DIAGNOSIS_CD like 'K85%') and 
 DIAGNOSIS_STATUS='Diagnosis of' and a.diag_date < c.indx_dt;

 select distinct * from ac_ehr_dx_acute_panc_pre_indx
 order by ptid, diag_date;

-- COMMAND ----------


create or replace table ac_ehr_acute_panc_proc_pre_indx as
select distinct a.ptid, a.encid, a.proc_date, a.proc_time, a.proc_code, a.proc_desc, b.indx_dt, b.yr_dt , b.diag_date from ac_ehr_proc_202303 a
inner join ac_ehr_dx_acute_panc_pre_indx b on a.ptid=b.ptid
where a.proc_date between b.diag_date and b.diag_date + 30
and proc_code in ('0FT44ZZ','0FT40ZZ','47480',
'47564',
'47612',
'48001',
'47563',
'47610',
'48000',
'47562',
'47620',
'47605',
'47600',
'47490')
order by a.ptid, a.PROC_DATE;

select distinct * from ac_ehr_acute_panc_proc_pre_indx
where 
order by ptid, PROC_DATE;

-- COMMAND ----------

select yr_dt, count(distinct ptid) from ac_ehr_acute_panc_proc_pre_indx
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Demographics

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_proc_demo as
select distinct a.ptid, a.indx_dt, a.yr_dt, b.BIRTH_YR, b.gender, b.race, b.ethnicity, b.region, b.division from ac_ehr_acute_panc_proc_2 a
inner join ac_ehr_patient_202303 b on a.ptid=b.ptid
order by 1;


select distinct * from ac_ehr_acute_panc_proc_demo
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_proc_demo_1 as
select distinct *, case when BIRTH_YR='1933 and Earlier' then 1933 else cast(birth_yr as int) end as Birth_YEAR,
case when race='Caucasian' and ethnicity in ('Not Hispanic','Unknown') then 'Caucasian'
when race='African American' and ethnicity in ('Not Hispanic','Unknown') then 'African American'
when race='Asian' and ethnicity in ('Not Hispanic','Unknown') then 'Asian'
when race in ('Asian','Caucasian','African American','Other/Unknown') and ethnicity ='Hispanic' then 'Hispanic'
else 'Other/Unknown' end as Race_new
from ac_ehr_acute_panc_proc_demo
order by 1;

select distinct * from ac_ehr_acute_panc_proc_demo_1;

-- COMMAND ----------

select yr_dt, Gender, count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
group by 1,2
order by 1,2;

select yr_dt, Race_new, count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
group by 1,2
order by 1,2;

select yr_dt, region, count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
group by 1,2
order by 1,2;

select yr_dt, division, count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
group by 1,2
order by 1,2;

-- COMMAND ----------

select yr_dt, case when yr_dt-Birth_YEAR < 18 then '0-17'
when yr_dt-Birth_YEAR >=18 and yr_dt-Birth_YEAR<41 then '18-40'
when yr_dt-Birth_YEAR >=41 and yr_dt-Birth_YEAR<65 then '41-64'
when yr_dt-Birth_YEAR >=65 then '65+' END as age,
count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
where yr_dt=2017
group by 1,2
order by 1,2;

select yr_dt, case when yr_dt-Birth_YEAR < 18 then '0-17'
when yr_dt-Birth_YEAR >=18 and yr_dt-Birth_YEAR<41 then '18-40'
when yr_dt-Birth_YEAR >=41 and yr_dt-Birth_YEAR<65 then '41-64'
when yr_dt-Birth_YEAR >=65 then '65+' END as age,
count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
where yr_dt=2018
group by 1,2
order by 1,2;

select yr_dt, case when yr_dt-Birth_YEAR < 18 then '0-17'
when yr_dt-Birth_YEAR >=18 and yr_dt-Birth_YEAR<41 then '18-40'
when yr_dt-Birth_YEAR >=41 and yr_dt-Birth_YEAR<65 then '41-64'
when yr_dt-Birth_YEAR >=65 then '65+' END as age,
count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
where yr_dt=2019
group by 1,2
order by 1,2;

select yr_dt, case when yr_dt-Birth_YEAR < 18 then '0-17'
when yr_dt-Birth_YEAR >=18 and yr_dt-Birth_YEAR<41 then '18-40'
when yr_dt-Birth_YEAR >=41 and yr_dt-Birth_YEAR<65 then '41-64'
when yr_dt-Birth_YEAR >=65 then '65+' END as age,
count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
where yr_dt=2020
group by 1,2
order by 1,2;

select yr_dt, case when yr_dt-Birth_YEAR < 18 then '0-17'
when yr_dt-Birth_YEAR >=18 and yr_dt-Birth_YEAR<41 then '18-40'
when yr_dt-Birth_YEAR >=41 and yr_dt-Birth_YEAR<65 then '41-64'
when yr_dt-Birth_YEAR >=65 then '65+' END as age,
count(distinct ptid) as pts from ac_ehr_acute_panc_proc_demo_1
where yr_dt=2021
group by 1,2
order by 1,2;
