-- Databricks notebook source
----------Import SES Medical Diagnosis records---------

-- drop table if exists ac_ehr_diag_202303;
-- CREATE TABLE ac_ehr_diag_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Diagnosis";

-- select distinct * from ac_ehr_diag_202303;

-- select max(diag_date) from ac_ehr_diag_202303;

-- drop table if exists ac_ehr_patient_202303;
-- CREATE TABLE ac_ehr_patient_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Patient";

-- drop table if exists ac_ehr_proc_202303;
-- CREATE TABLE ac_ehr_proc_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Procedure";

-- select distinct * from ac_ehr_proc_202303;

-- drop table if exists ac_ehr_obs_202303;
-- CREATE TABLE ac_ehr_obs_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Observation";

-- select distinct * from ac_ehr_obs_202303;

drop table if exists ac_ehr_lab_202303;
CREATE TABLE ac_ehr_lab_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Lab";

select distinct * from ac_ehr_lab_202303;


-- COMMAND ----------

drop table if exists ac_ehr_dx_act_pancrtis_1_full;
create or replace table ac_ehr_dx_act_pancrtis_1_full as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE
from ac_ehr_diag_202303 a 
where a.diag_date>='2017-01-01' AND
(a.DIAGNOSIS_CD='5770' or a.DIAGNOSIS_CD like 'K85%')
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_act_pancrtis_1_full
order by ptid, diag_date;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_act_pancrtis_1_full; ---511715

select year(diag_date) as yr, count(distinct ptid) from ac_ehr_dx_act_pancrtis_1_full
where year(diag_date)>=2015
group by 1
order by 1;

-- yr	count(DISTINCT ptid)
-- 2015	68150
-- 2016	71316
-- 2017	75327
-- 2018	60743
-- 2019	57168
-- 2020	46455
-- 2021	37495
-- 2022	15411

-- COMMAND ----------

drop table if exists ac_ehr_dx_ac_pancrtis_indx;

create table ac_ehr_dx_ac_pancrtis_indx as
select distinct ptid, year(diag_date) as yr_dt, min(diag_date) as indx_dt, count(distinct diag_date) as diag_cnt from ac_ehr_dx_act_pancrtis_1_full
group by 1, 2
order by 1,2

;

select * from ac_ehr_dx_ac_pancrtis_indx;

-- COMMAND ----------

create or replace table ac_ehr_dx_ac_pancrtis_indx_1 as
select distinct * from ac_ehr_dx_ac_pancrtis_indx
where diag_cnt>=2
order by 1;

select distinct * from ac_ehr_dx_ac_pancrtis_indx_1
order by 1;

-- COMMAND ----------

-- create or replace table ac_dod_dx_act_pancrtis_2 as
-- select distinct a.*,b.indx_dt from ac_dod_dx_act_pancrtis_1 a
-- left join ac_cdm_dx_ac_pancrtis_indx b on a.patid=b.patid and a.yr_dt=b.yr_dt
-- order by patid, fst_dt;

select distinct * from ac_dod_dx_act_pancrtis_2
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_clm as
select distinct a.*, b.ADMIT_CHAN, b.ADMIT_TYPE, b.bill_prov, 
b.charge, b.cob, b.coins, b.conf_id, b.copay, b.deduct, b.lst_dt, b.pos, b.tos_cd
from ac_dod_dx_act_pancrtis_2 a left join ac_dod_2303_med_claims b on a.patid=b.patid and a.clmid=b.clmid and a.fst_dt=b.fst_dt;

select distinct * from ac_dod_dx_act_pancrtis_clm
order by patid, fst_dt;

-- COMMAND ----------

describe table ac_dod_dx_act_pancrtis_clm;

describe table ac_dod_dx_act_panc_IP_conf; 

-- COMMAND ----------

select year(fst_dt) as yr, count(distinct patid) from ac_dod_dx_act_pancrtis_clm
where pos in ('21')
group by 1
order by 1

-- COMMAND ----------

-- MAGIC %md #### joining with inpatient conf table to check patients

-- COMMAND ----------

select yr_Dt, count(distinct a.patid) as cnts from ac_dod_dx_act_pancrtis_2 a
inner join ac_dod_2303_IP_confin b on a.patid=b.patid
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_dod_dx_act_panc_IP_conf as
select distinct * from 
ac_dod_2303_IP_confin
where admit_date>='2015-01-01' and (diag1='5770' or diag1 like 'K85%' or diag2='5770' or diag2 like 'K85%' or diag3='5770' or diag3 like 'K85%')
;

select distinct * from ac_dod_dx_act_panc_IP_conf
order by patid, admit_date;

-- COMMAND ----------

select distinct * from ac_dod_dx_act_panc_IP_conf
where patid='33003299715'
order by patid, admit_date;


-- COMMAND ----------

select year(admit_date) as yr, count(distinct patid) from ac_dod_dx_act_panc_IP_conf
where los>=1
group by 1
order by 1;


-- COMMAND ----------

-- MAGIC %md #### length of stay

-- COMMAND ----------

create or replace table ac_dod_dx_act_panc_IP_conf_LOS as
select distinct patid, year(admit_date) as yr, sum(los) as Sum_LOS
from ac_dod_dx_act_panc_IP_conf
group by 1,2
order by 1,2;

select distinct * from ac_dod_dx_act_panc_IP_conf_LOS
where patid='33003299715'
order by patid, yr;

-- COMMAND ----------

select  yr, mean(Sum_LOS) from ac_dod_dx_act_panc_IP_conf_LOS
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Total members

-- COMMAND ----------

create or replace table ac_ehr_patient_202303_full_pts as
select distinct a.*,left(FIRST_MONTH_ACTIVE,4) as first_mnth_yr, right(FIRST_MONTH_ACTIVE,2) as first_mnth,
left(LAST_MONTH_ACTIVE,4) as last_mnth_yr, right(LAST_MONTH_ACTIVE,2) as last_mnth from ac_ehr_patient_202303 a;

select distinct * from ac_ehr_patient_202303_full_pts;

create or replace table ac_ehr_patient_202303_full_pts_1 as
select distinct *, cast(concat(first_mnth_yr,'-',first_mnth,'-','01') as date) as first_month_active_new,
cast(concat(last_mnth_yr,'-',last_mnth,'-','01') as date) as last_month_active_new from ac_ehr_patient_202303_full_pts;

select distinct * from ac_ehr_patient_202303_full_pts_1;


-- COMMAND ----------

create or replace table ac_ehr_total_enrolee_2303 as
select '1 # of patients with cont coverage in 2015' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2015-01-01' and '2015-12-31'<=last_month_active_new
UNION
select '2 # of patients with cont coverage in 2016' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2016-01-01' and '2016-12-31'<=last_month_active_new
UNION
select '3 # of patients with cont coverage in 2017' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2017-01-01' and '2017-12-31'<=last_month_active_new
UNION
select '4 # of patients with cont coverage in 2018' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2018-01-01' and '2018-12-31'<=last_month_active_new
UNION
select '5 # of patients with cont coverage in 2019' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2019-01-01' and '2019-12-31'<=last_month_active_new
UNION
select '6 # of patients with cont coverage in 2020' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2020-01-01' and '2020-12-31'<=last_month_active_new
UNION
select '7 # of patients with cont coverage in 2021' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2021-01-01' and '2021-12-31'<=last_month_active_new
UNION
select '8 # of patients with cont coverage in 2022' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2022-01-01' and '2022-12-31'<=last_month_active_new
UNION
select '9 # of patients with cont coverage in 2023' as cat, count(distinct ptid) as cnts from ac_ehr_patient_202303_full_pts_1
where  first_month_active_new<='2023-01-01' and '2023-12-31'<=last_month_active_new
order by cat;

select distinct * from ac_ehr_total_enrolee_2303
order by cat;


-- COMMAND ----------

-- MAGIC %md #### Incidence population

-- COMMAND ----------

drop table if exists ac_dod_dx_act_pancrtis_full;

create table ac_dod_dx_act_pancrtis_full as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                ,year(fst_dt) as yr_dt
from ac_dod_2303_med_diag a
where  (a.diag='5770' or a.diag like 'K85%')
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_act_pancrtis_full
order by patid, fst_dt;

-- COMMAND ----------

-- MAGIC %md #### Indx date

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_full_indx as
select distinct patid, min(fst_dt) as Index_date from ac_dod_dx_act_pancrtis_full
group by 1
order by 1;

select distinct * from ac_dod_dx_act_pancrtis_full_indx
order by 1;

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_incidnc as
select '1 # of patients in 2015' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2015-01-01' and '2015-12-31'
UNION
select '2 # of patients  in 2016' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2016-01-01' and '2016-12-31'
UNION
select '3 # of patients in 2017' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2017-01-01' and '2017-12-31'
UNION
select '4 # of patients in 2018' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2018-01-01' and '2018-12-31'
UNION
select '5 # of patients in 2019' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2019-01-01' and '2019-12-31'
UNION
select '6 # of patients in 2020' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2020-01-01' and '2020-12-31'
UNION
select '7 # of patients in 2021' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2021-01-01' and '2021-12-31'
UNION
select '8 # of patients in 2022' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2022-01-01' and '2022-12-31'
UNION
select '9 # of patients in 2023' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx
where  Index_date between '2023-01-01' and '2023-12-31'
order by cat;

select distinct * from ac_dod_dx_act_pancrtis_incidnc
order by cat;


-- COMMAND ----------

-- MAGIC %md #### Total incidence population

-- COMMAND ----------

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2015-01-01' and '2015-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2014-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2016-01-01' and '2016-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2015-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2017-01-01' and '2017-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2016-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2018-01-01' and '2018-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2017-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2019-01-01' and '2019-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2018-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2020-01-01' and '2020-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2019-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2021-01-01' and '2021-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2020-12-31' );


select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2022-01-01' and '2022-12-31'<=eligend
and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
where fst_dt<='2021-12-31' );
