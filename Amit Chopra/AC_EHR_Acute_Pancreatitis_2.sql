-- Databricks notebook source
-- MAGIC %md #### Check for cholecystectomy

-- COMMAND ----------


create or replace table ac_ehr_acute_panc_proc_1 as
select distinct a.ptid, a.encid, a.proc_date, a.proc_time, a.proc_code, a.proc_desc, b.indx_dt, b.yr_dt from ac_ehr_proc_202303 a
inner join ac_ehr_dx_ac_pancrtis_indx_1 b on a.ptid=b.ptid
where a.proc_date between b.indx_dt and b.indx_dt + 30
order by a.ptid, a.PROC_DATE;

select distinct * from ac_ehr_acute_panc_proc_1
order by ptid, PROC_DATE;

-- COMMAND ----------

select proc_desc, proc_code, count(distinct ptid) from ac_ehr_acute_panc_proc_1
group by 1, 2
order by 3 desc;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_proc_2 as
select distinct *, year(proc_date) as yr_proc from ac_ehr_acute_panc_proc_1
where proc_code in ('0FT44ZZ','0FT40ZZ','47480',
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
order by ptid, proc_date;

select distinct * from ac_ehr_acute_panc_proc_2
order by ptid, proc_date;

-- COMMAND ----------

select distinct * from ac_ehr_acute_panc_proc_2
where indx_dt='2017-12-30'
order by ptid, proc_date

-- COMMAND ----------


select distinct yr_dt, count(distinct ptid) from ac_ehr_acute_panc_proc_2
group by 1
order by 1;


select distinct count(distinct ptid) from ac_ehr_acute_panc_proc_2
where yr_dt between 2017 and 2021;

-- COMMAND ----------

-- MAGIC %md #### CHecking patients with different comorbidities

-- COMMAND ----------

drop table if exists ac_ehr_dx_acute_panc_comorb;
create or replace table ac_ehr_dx_acute_panc_comorb as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE, c.yr_dt, c.indx_dt
from ac_ehr_diag_202303 a
-- a join ty00_all_dx_comorb b
-- on a.DIAGNOSIS_CD=b.code
inner join ac_ehr_acute_panc_proc_2 c on a.ptid=c.ptid and year(a.diag_date)=c.yr_dt
where
 DIAGNOSIS_STATUS='Diagnosis of';

 select distinct * from ac_ehr_dx_acute_panc_comorb
 order by ptid, diag_date;

-- COMMAND ----------

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb a
join ty00_all_dx_comorb b
on a.DIAGNOSIS_CD=b.code
where dx_name in ('T1DM')
group by 1
order by 1;

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb a
join ty00_all_dx_comorb b
on a.DIAGNOSIS_CD=b.code
where dx_name in ('T2DM')
group by 1
order by 1;

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD like ('I10%') or DIAGNOSIS_CD like ('I11%') or DIAGNOSIS_CD like ('I12%') or DIAGNOSIS_CD like ('I13%')
or DIAGNOSIS_CD like ('I15%')
group by 1
order by 1;

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD in ('E785')
group by 1
order by 1;

--CHD
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD like ('I25%') or DIAGNOSIS_CD like ('I24%') or DIAGNOSIS_CD like ('I23%') or DIAGNOSIS_CD like ('I22%')
or DIAGNOSIS_CD like ('I21%') or DIAGNOSIS_CD like ('I20%') or DIAGNOSIS_CD like ('I11%') or DIAGNOSIS_CD like ('I65%')
or DIAGNOSIS_CD like ('I66%') or DIAGNOSIS_CD like ('I70%') or DIAGNOSIS_CD like ('I71%') or DIAGNOSIS_CD like ('I72%')
or DIAGNOSIS_CD like ('I73%') or DIAGNOSIS_CD like ('I74%')
group by 1
order by 1;

--CKD

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD like ('N18%') or diagnosis_cd in ('I120', 'I129', 'I130', 'I1310', 'I1311', 'I132','E102', 'E112', 'E1121', 'E1122', 'E1129','N250', 'N251', 'N2581', 'N2589', 'N259')
group by 1
order by 1;

--Billiary DIsease
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD like ('K851%')
group by 1
order by 1;

--Billiary DIsease
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD like ('K80%') or DIAGNOSIS_CD like ('K81%') or DIAGNOSIS_CD like ('K82%') or DIAGNOSIS_CD like ('K83%')
or DIAGNOSIS_CD like ('K87%')
group by 1
order by 1;


--Chronic Pancreatitis
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD in ('K861','K860')
group by 1
order by 1;

--CHF
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where substr(DIAGNOSIS_CD,1,3) in  ('I11', 'I42', 'I43', 'I50', 'J81') or diagnosis_cd in ('I130')
group by 1
order by 1;

--Cerebrovascular disease
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where substr(DIAGNOSIS_CD,1,3) in  ('G45', 'G46', 'I60', 'I61', 'I62','I63','I65',
'I66','I67','I68','I69')
group by 1
order by 1;

--Peripheral vascular disease

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where substr(DIAGNOSIS_CD,1,3) in  ('I65', 'I70', 'I71', 'I72', 'I73','I74','I75',
'I76','I77') or substr(DIAGNOSIS_CD,1,4) in ('K550','K551')
group by 1
order by 1;

--Obesity

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where substr(DIAGNOSIS_CD,1,3) in  ('E65', 'E66')
group by 1
order by 1;

--HIV

select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD in  ('B20','B21','B22','B23','B24','B249','B200',	'B201',	'B202',	'B203',	'B204',	'B205',	'B206',	'B207',	'B208',	'B209',	'B210',	'B211',	'B212',	'B213',	'B217',	'B218',	'B219',	'B220',	'B221',	'B222',	'B227',	'B230',	'B231',	'B232',	'B238')
group by 1
order by 1;

--pyschoactive drug
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where substr(DIAGNOSIS_CD,1,3) in  ('F19')
group by 1
order by 1;

--Billiary DIsease
select yr_dt, count(distinct ptid) from ac_ehr_dx_acute_panc_comorb
where DIAGNOSIS_CD like ('K851%')
group by 1
order by 1;


-- COMMAND ----------

select distinct diagnosis_cd from ac_ehr_dx_acute_panc_comorb
where substr(DIAGNOSIS_CD,1,3) in  ('I65', 'I70', 'I71', 'I72', 'I73','I74','I75',
'I76','I77') or substr(DIAGNOSIS_CD,1,4) in ('K550','K551')

-- COMMAND ----------

-- MAGIC %md #### Checking the observation table for alcohol, BMI and Smoke 

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_obs_1 as
select distinct a.*, b.yr_dt, b.indx_dt  from ac_ehr_obs_202303 a
inner join ac_ehr_acute_panc_proc_2 b on a.ptid=b.ptid
where a.OBS_TYPE in ('BMI','ALCOHOL','SMOKE','SMOKE_CESS_CONSULT')
order by a.PTID, a.OBS_DATE;

select distinct * from ac_ehr_acute_panc_obs_1
order by ptid, obs_date;

-- COMMAND ----------

-- MAGIC %md #### Checking BMI

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_obs_BMI_1 as
select distinct * from ac_ehr_acute_panc_obs_1
where obs_type='BMI' and obs_date between indx_dt - 180 and indx_dt
order by ptid, obs_date;

select distinct * from ac_ehr_acute_panc_obs_BMI_1
where ptid='PT583616316'
order by ptid, obs_date;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_obs_BMI_max_dt as
select distinct ptid, max(concat(obs_date," ",OBS_TIME)) as max_dt from ac_ehr_acute_panc_obs_BMI_1
group by 1
order by 1;

select distinct * from ac_ehr_acute_panc_obs_BMI_max_dt
where ptid='PT079097213'
order by 1;


-- COMMAND ----------

create or replace table ac_ehr_acute_panc_obs_BMI_2 as
select distinct a.*, cast(obs_result as FLOAT) as obs_val from ac_ehr_acute_panc_obs_BMI_1 a
inner join ac_ehr_acute_panc_obs_BMI_max_dt b on a.ptid=b.ptid and concat(a.obs_date," ",a.OBS_TIME)=b.max_dt
order by ptid, obs_date;


select distinct * from ac_ehr_acute_panc_obs_BMI_2
-- where obs_date='2007-02-20'
order by 1;

-- COMMAND ----------

select yr_dt, count(distinct ptid) from ac_ehr_acute_panc_obs_BMI_2
where obs_type="BMI"
group by 1
order by 1;

-- yr_dt	count(DISTINCT ptid)
-- 2017	5130
-- 2018	4556
-- 2019	4449
-- 2020	3441
-- 2021	2633
-- 2022	889

-- COMMAND ----------

select yr_dt, count(distinct ptid) from ac_ehr_acute_panc_obs_BMI_2
where obs_val>25
group by 1
order by 1;

select yr_dt, count(distinct ptid) from ac_ehr_acute_panc_obs_BMI_2
where obs_val>30
group by 1
order by 1;


-- COMMAND ----------

-- MAGIC %md #### alcohol use 

-- COMMAND ----------

create or replace table ac_ehr_act_panc_obs_alcohol_1 as
select distinct * from ac_ehr_acute_panc_obs_1
where obs_type='ALCOHOL' and OBS_DATE between indx_dt - 366 and indx_dt
order by ptid, obs_date;

select distinct * from ac_ehr_act_panc_obs_alcohol_1
order by ptid, obs_date;

-- COMMAND ----------

create or replace table ac_ehr_acute_panc_obs_alcohol_max_dt as
select distinct ptid, max(concat(obs_date," ",OBS_TIME)) as max_dt from ac_ehr_act_panc_obs_alcohol_1
group by 1
order by 1;

select distinct * from ac_ehr_acute_panc_obs_alcohol_max_dt
-- where ptid='PT079097213'
order by 1;


-- COMMAND ----------

create or replace table ac_ehr_act_panc_obs_alcohol_2 as
select distinct a.*, cast(obs_result as FLOAT) as obs_val from ac_ehr_act_panc_obs_alcohol_1 a
inner join ac_ehr_acute_panc_obs_alcohol_max_dt b on a.ptid=b.ptid and concat(a.obs_date," ",a.OBS_TIME)=b.max_dt
order by ptid, obs_date;


select distinct * from ac_ehr_act_panc_obs_alcohol_2
-- where obs_date='2007-02-20'
order by ptid, obs_date;

-- COMMAND ----------

select yr_dt, obs_result, count(distinct ptid) from ac_ehr_act_panc_obs_alcohol_2
group by 1, 2
order by 1, 3 desc;

-- COMMAND ----------

-- MAGIC %md #### Smoking use

-- COMMAND ----------

create or replace table ac_ehr_act_panc_obs_smoke_1 as
select distinct * from ac_ehr_acute_panc_obs_1
where obs_type='SMOKE' and OBS_DATE between indx_dt - 366 and indx_dt
order by ptid, obs_date;

select distinct * from ac_ehr_act_panc_obs_smoke_1
order by ptid, obs_date;

-- COMMAND ----------

create or replace table ac_ehr_act_panc_obs_smoke_max_dt as
select distinct ptid, max(concat(obs_date," ",OBS_TIME)) as max_dt from ac_ehr_act_panc_obs_smoke_1
group by 1
order by 1;

select distinct * from ac_ehr_act_panc_obs_smoke_max_dt
-- where ptid='PT079097213'
order by 1;


-- COMMAND ----------

create or replace table ac_ehr_act_panc_obs_smoke_2 as
select distinct a.*, cast(obs_result as FLOAT) as obs_val from ac_ehr_act_panc_obs_smoke_1 a
inner join ac_ehr_act_panc_obs_smoke_max_dt b on a.ptid=b.ptid and concat(a.obs_date," ",a.OBS_TIME)=b.max_dt
order by ptid, obs_date;


select distinct * from ac_ehr_act_panc_obs_smoke_2
-- where obs_date='2007-02-20'
order by ptid, obs_date;

-- COMMAND ----------

select yr_dt, obs_result, count(distinct ptid) from ac_ehr_act_panc_obs_smoke_2
group by 1, 2
order by 1, 3 desc;
