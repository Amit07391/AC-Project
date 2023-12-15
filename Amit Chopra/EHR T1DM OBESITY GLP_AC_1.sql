-- Databricks notebook source


drop table if exists ac_dod_2305_med_diag;
create table ac_dod_2305_med_diag using delta location 'dbfs:/mnt/optumclin/202305/ontology/base/dod/Medical Diagnosis';

select distinct * from ac_dod_2305_med_diag

-- COMMAND ----------

create or replace table ac_dod_obese_diag_20_22 as
select distinct * from ac_dod_2305_med_diag
where diag like 'E66%' and FST_DT>='2020-01-01' and FST_DT <='2022-12-31'
order by patid, FST_DT;

select distinct * from ac_dod_obese_diag_20_22
order by patid, FST_DT;

-- COMMAND ----------

select count(distinct patid) from ac_dod_obese_diag_20_22

-- COMMAND ----------

drop table if exists ac_ehr_dx_t1dm_2021;
create or replace table ac_ehr_dx_t1dm_2021 as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE
from ac_ehr_dx_t1dm_secnd_16_22 a
where a.DIAG_DATE>='2021-01-01' AND a.DIAG_DATE<='2021-12-31' AND
(DIAGNOSIS_CD in (select code from ty00_all_dx_comorb where dx_name='T1DM'))
order by ptid, diag_date;

select distinct * from ac_ehr_dx_t1dm_2021
order by ptid, diag_date;

-- COMMAND ----------

create or replace table ac_ehr_dx_t2dm_2021 as
select distinct * from ac_ehr_dx_t2dm_16_22
where diag_date>='2021-01-01' AND diag_date<='2021-12-31'
order by ptid, diag_date;

select distinct * from ac_ehr_dx_t2dm_2021
order by ptid, diag_date;

-- COMMAND ----------

create or replace table ac_ehr_t1d_t2d_ratio as
select distinct a.ptid, count(distinct a.diag_date) as t1d_cnts, count(distinct b.diag_date) as t2d_cnts from
(select distinct ptid, diag_date from ac_ehr_dx_t1dm_2021 where diag_date between '2021-01-01' and '2021-12-31') a
left join (select distinct ptid, diag_date from ac_ehr_dx_t2dm_2021 where diag_date between '2021-01-01' and '2021-12-31') b on a.ptid=b.ptid
group by 1
order by 1;

select distinct * from ac_ehr_t1d_t2d_ratio;

-- COMMAND ----------

create or replace table ac_ehr_t1d_t2d_ratio_2 as
select distinct *, t2d_cnts/t1d_cnts as ratio from ac_ehr_t1d_t2d_ratio
where t2d_cnts/t1d_cnts>=2
order by 1;

select distinct * from ac_ehr_t1d_t2d_ratio_2
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_t1d_exld_t2d_2021 as
select distinct * from ac_ehr_dx_t1dm_2021
where ptid not in (select distinct ptid from ac_ehr_t1d_t2d_ratio_2);

select count(distinct ptid) from ac_ehr_t1d_exld_t2d_2021
where diag_date between '2021-01-01' and '2021-12-31';

-- COMMAND ----------

drop table if exists ac_ehr_t1dm_trt;

create table ac_ehr_t1dm_trt as
select distinct a.ptid, a.dt_1st_dx_t1dm, b.dt_1st_rx_basal, b.n_basal, c.dt_1st_rx_bolus, c.n_bolus, d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro, h.first_month_active_new, h.last_month_active_new
from (select ptid, min(diag_date) as dt_1st_dx_t1dm from ac_ehr_t1d_exld_t2d_2021 where diag_date between '2021-01-01' and '2021-12-31' group by ptid) a
left join (select ptid, min(rxdate) as dt_1st_rx_basal, count(distinct rxdate) as n_basal from ac_rx_anti_dm where rxdate between '2021-01-01' and '2021-12-31' and rx_type='Basal' group by ptid) b on a.ptid=b.ptid
left join (select ptid, min(rxdate) as dt_1st_rx_bolus, count(distinct rxdate) as n_bolus from ac_rx_anti_dm where rxdate between '2021-01-01' and '2021-12-31' and rx_type='Bolus' group by ptid) c on a.ptid=c.ptid
left join (select ptid, min(rxdate) as dt_1st_rx_glp1, count(distinct rxdate) as n_glp1  from ac_rx_anti_dm where rxdate between '2021-01-01' and '2021-12-31' and rx_type='GLP1' and lcase(brnd_nm) not like '%mounjaro%' group by ptid) d on a.ptid=d.ptid
left join (select ptid, min(rxdate) as dt_1st_rx_mounjaro, count(distinct rxdate) as n_mounjaro from ac_rx_anti_dm where rxdate between '2021-01-01' and '2021-12-31' and lcase(brnd_nm) like '%mounjaro%' group by ptid) e on a.ptid=e.ptid
left join (select distinct ptid, first_month_active_new, last_month_active_new from ac_ehr_patient_202305_full_pts_2) h on a.ptid=h.ptid and a.dt_1st_dx_t1dm between h.first_month_active_new and h.last_month_active_new
order by a.ptid;

select * from ac_ehr_t1dm_trt;

-- COMMAND ----------

create or replace table ac_ehr_obs_bmi_t1d as
select distinct a.*,cast(obs_result as double) as result, b.dt_1st_dx_t1dm, b.first_month_active_new, b.last_month_active_new from ac_ehr_obs_202305 a
inner join ac_ehr_t1dm_trt b on a.PTID=b.ptid
where obs_type like '%BMI%' and (OBS_DATE between '2020-01-01' and '2022-12-31')
and (b.first_month_active_new<='2021-01-01' and '2021-12-31'<=b.last_month_active_new)
order by a.ptid, a.OBS_DATE;

select distinct * from ac_ehr_obs_bmi_t1d
order by ptid, obs_date;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_obs_bmi_t1d;
-- select distinct obs_result from ac_ehr_obs_bmi_t1d;

-- COMMAND ----------

create or replace table ac_ehr_obs_bmi_max_value as
select distinct ptid, max(result) as max_value from ac_ehr_obs_bmi_t1d
group by 1
order by 1;

select distinct * from ac_ehr_obs_bmi_max_value
order by 1;

create or replace table ac_ehr_obs_bmi_max_value_gt_30 as
select distinct * from ac_ehr_obs_bmi_max_value
where max_value>30; 

select count(distinct ptid) from ac_ehr_obs_bmi_max_value_gt_30;

-- COMMAND ----------

drop table if exists ac_ehr_t1dm_summary;

create table ac_ehr_t1dm_summary as
select count(distinct a.ptid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.ptid) as n_nasal_bolus
, 100*count(distinct b.ptid)/count(distinct a.ptid) as pct_nasal_bolus, count(distinct c.ptid) as n_glp1_mounjaro, 100*count(distinct c.ptid)/count(distinct b.ptid) as pct_glp1_mounjaro
, 100*count(distinct c.ptid)/count(distinct a.ptid) as pct_glp1_mounjaro_t1dm
from (select ptid, dt_1st_dx_t1dm, first_month_active_new, last_month_active_new from ac_ehr_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select ptid from ac_ehr_t1dm_trt where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) b on a.ptid=b.ptid
left join (select ptid from ac_ehr_t1dm_trt where isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) c on a.ptid=c.ptid
inner join ac_ehr_obs_bmi_max_value_gt_30 d on a.ptid=d.ptid
where a.first_month_active_new<='2021-01-01' and '2021-12-31'<=a.last_month_active_new
;

select * from ac_ehr_t1dm_summary;


-- COMMAND ----------

drop table if exists ac_ehr_t1dm_wo_bmi_summary;

create table ac_ehr_t1dm_wo_bmi_summary as
select count(distinct a.ptid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.ptid) as n_nasal_bolus
, 100*count(distinct b.ptid)/count(distinct a.ptid) as pct_nasal_bolus, count(distinct c.ptid) as n_glp1_mounjaro, 100*count(distinct c.ptid)/count(distinct b.ptid) as pct_glp1_mounjaro
, 100*count(distinct c.ptid)/count(distinct a.ptid) as pct_glp1_mounjaro_t1dm
from (select ptid, dt_1st_dx_t1dm, first_month_active_new, last_month_active_new from ac_ehr_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select ptid from ac_ehr_t1dm_trt where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) b on a.ptid=b.ptid
left join (select ptid from ac_ehr_t1dm_trt where isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) c on a.ptid=c.ptid
where a.first_month_active_new<='2021-01-01' and '2021-12-31'<=a.last_month_active_new
;

select * from ac_ehr_t1dm_wo_bmi_summary;


-- COMMAND ----------

drop table if exists ac_t1dm_summary_least2;

create table ac_ehr_t1dm_summary_least2 as
select count(distinct a.ptid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.ptid) as n_nasal_bolus
, 100*count(distinct b.ptid)/count(distinct a.ptid) as pct_nasal_bolus, count(distinct c.ptid) as n_glp1_mounjaro, 100*count(distinct c.ptid)/count(distinct b.ptid) as pct_glp1_mounjaro
, 100*count(distinct c.ptid)/count(distinct a.ptid) as pct_glp1_mounjaro_t1dm
from (select ptid, dt_1st_dx_t1dm, first_month_active_new, last_month_active_new from ac_ehr_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select ptid from ac_ehr_t1dm_trt where (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) and (n_bolus>1 or n_basal>1 or (n_bolus=1 and n_basal=1 ))) b on a.ptid=b.ptid
left join (select ptid from ac_ehr_t1dm_trt where (isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (n_glp1>1 or n_mounjaro>1 or (n_glp1=1 and n_mounjaro=1))) c on a.ptid=c.ptid
inner join ac_ehr_obs_bmi_max_value_gt_30 d on a.ptid=d.ptid
where a.first_month_active_new<='2021-01-01' and '2021-12-31'<=a.last_month_active_new
;

select * from ac_ehr_t1dm_summary_least2;


-- COMMAND ----------

drop table if exists ac_t1dm_wo_bmi_summary_least2;

create table ac_t1dm_wo_bmi_summary_least2 as
select count(distinct a.ptid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.ptid) as n_nasal_bolus
, 100*count(distinct b.ptid)/count(distinct a.ptid) as pct_nasal_bolus, count(distinct c.ptid) as n_glp1_mounjaro, 100*count(distinct c.ptid)/count(distinct b.ptid) as pct_glp1_mounjaro
, 100*count(distinct c.ptid)/count(distinct a.ptid) as pct_glp1_mounjaro_t1dm
from (select ptid, dt_1st_dx_t1dm, first_month_active_new, last_month_active_new from ac_ehr_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select ptid from ac_ehr_t1dm_trt where (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) and (n_bolus>1 or n_basal>1 or (n_bolus=1 and n_basal=1 ))) b on a.ptid=b.ptid
left join (select ptid from ac_ehr_t1dm_trt where (isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (n_glp1>1 or n_mounjaro>1 or (n_glp1=1 and n_mounjaro=1))) c on a.ptid=c.ptid
where a.first_month_active_new<='2021-01-01' and '2021-12-31'<=a.last_month_active_new
;

select * from ac_t1dm_wo_bmi_summary_least2;


-- COMMAND ----------

drop table if exists ac_ehr_t2dm_trt_bas_bol;

create table ac_ehr_t2dm_trt_bas_bol as
select distinct a.ptid, a.dt_1st_dx_t2dm, b.dt_1st_rx_basal, b.n_basal, c.dt_1st_rx_bolus, c.n_bolus, h.first_month_active_new,h.last_month_active_new
-- d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro
from (select ptid, min(diag_date) as dt_1st_dx_t2dm from ac_ehr_dx_t2dm_2021 where diag_date between '2021-01-01' and '2021-12-31' group by ptid) a
left join (select ptid, min(rxdate) as dt_1st_rx_basal, count(distinct rxdate) as n_basal from ac_rehr_x_anti_dm_bas_bol_1 where rxdate between '2021-01-01' and '2021-12-31' and rx_type='Basal' group by ptid) b on a.ptid=b.ptid
left join (select ptid, min(rxdate) as dt_1st_rx_bolus, count(distinct rxdate) as n_bolus from ac_rehr_x_anti_dm_bas_bol_1 where rxdate between '2021-01-01' and '2021-12-31' and rx_type='Bolus' group by ptid) c on a.ptid=c.ptid
-- left join (select ptid, min(rxdate) as dt_1st_rx_glp1, count(distinct rxdate) as n_glp1  from ty41_rx_anti_dm where rxdate between '2021-01-01' and '2021-12-31' and rx_type='GLP1' and lcase(brnd_nm) not like '%mounjaro%' group by ptid) d on a.ptid=d.ptid
-- left join (select ptid, min(rxdate) as dt_1st_rx_mounjaro, count(distinct rxdate) as n_mounjaro from ty41_rx_anti_dm where rxdate between '2021-01-01' and '2021-12-31' and lcase(brnd_nm) like '%mounjaro%' group by ptid) e on a.ptid=e.ptid
left join (select distinct ptid, first_month_active_new, last_month_active_new from ac_ehr_patient_202305_full_pts_2) h on a.ptid=h.ptid and a.dt_1st_dx_t2dm between h.first_month_active_new and h.last_month_active_new
order by a.ptid;

select * from ac_ehr_t2dm_trt_bas_bol
-- where ptid='33010960959';
order by ptid

-- COMMAND ----------

drop table if exists ac_ehr_t2dm_trt_bas_bol_GLP;

create table ac_ehr_t2dm_trt_bas_bol_GLP as
select distinct a.ptid, a.dt_1st_dx_t2dm,b.drug_flg, h.first_month_active_new,h.last_month_active_new
-- d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro
from (select ptid, min(diag_date) as dt_1st_dx_t2dm from ac_ehr_dx_t2dm_2021 where diag_date between '2021-01-01' and '2021-12-31' group by ptid) a
left join (select ptid, 1 as drug_flg from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_2 where rxdate between '2021-01-01' and '2021-12-31'  group by ptid) b on a.ptid=b.ptid
left join (select distinct ptid, first_month_active_new, last_month_active_new from ac_ehr_patient_202305_full_pts_2) h on a.ptid=h.ptid and a.dt_1st_dx_t2dm between h.first_month_active_new and h.last_month_active_new
order by a.ptid;

select * from ac_ehr_t2dm_trt_bas_bol_GLP;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_t2dm_trt_bas_bol; -- 1616041
select count(distinct ptid) from ac_ehr_t2dm_trt_bas_bol_GLP
where first_month_active_new<='2021-01-01' and '2021-12-31'<=last_month_active_new; -- 1092048

-- COMMAND ----------

create or replace table ac_ehr_obs_bmi_t2d as
select distinct a.*,cast(obs_result as double) as result, b.dt_1st_dx_t2dm, b.first_month_active_new, b.last_month_active_new from ac_ehr_obs_202305 a
inner join ac_ehr_t2dm_trt_bas_bol b on a.PTID=b.ptid
where upper(obs_type) like '%BMI%' and (OBS_DATE between '2020-01-01' and '2022-12-31')
and (b.first_month_active_new<='2021-01-01' and '2021-12-31'<=b.last_month_active_new)
order by a.ptid, a.OBS_DATE;

select distinct * from ac_ehr_obs_bmi_t2d
order by ptid, obs_date;

-- COMMAND ----------

create or replace table ac_ehr_obs_bmi_t2d_max_value as
select distinct ptid, max(result) as max_value from ac_ehr_obs_bmi_t2d
group by 1
order by 1;

select distinct * from ac_ehr_obs_bmi_t2d_max_value
order by 1;

create or replace table ac_ehr_obs_bmi_t2d_max_value_gt_30 as
select distinct * from ac_ehr_obs_bmi_t2d_max_value
where max_value>30;

select count(distinct ptid) from ac_ehr_obs_bmi_t2d_max_value_gt_30;

-- COMMAND ----------

drop table if exists ac_ehr_t2dm_summary_bas_bol;

create table ac_ehr_t2dm_summary_bas_bol as
select count(distinct a.ptid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.ptid) as n_basal_bolus
, 100*count(distinct b.ptid)/count(distinct a.ptid) as pct_basal_bolus
from (select ptid, dt_1st_dx_t2dm, first_month_active_new,last_month_active_new from ac_ehr_t2dm_trt_bas_bol where isnotnull(dt_1st_dx_t2dm)) a
left join (select ptid, sum(n_basal)+sum(n_bolus) as n_fill_basal_bolus from ac_ehr_t2dm_trt_bas_bol where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus) group by ptid) b on a.ptid=b.ptid
inner join ac_ehr_obs_bmi_t2d_max_value_gt_30 c on a.ptid=c.ptid
where a.first_month_active_new<='2021-01-01' and '2021-12-31'<=a.last_month_active_new
;

select * from ac_ehr_t2dm_summary_bas_bol;

-- COMMAND ----------

drop table if exists ac_ehr_t2dm_summary_bas_bol_GLP;

create table ac_ehr_t2dm_summary_bas_bol_GLP as
select count(distinct a.ptid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.ptid) as n_basal_bolus, count(distinct c.ptid) as n_basal_bolus_GLP
, 100*count(distinct b.ptid)/count(distinct a.ptid) as pct_basal_bolus,
100*count(distinct c.ptid)/count(distinct b.ptid) as pct_basal_bolus_GLP
from (select ptid, dt_1st_dx_t2dm, first_month_active_new,last_month_active_new from ac_ehr_t2dm_trt_bas_bol_GLP where isnotnull(dt_1st_dx_t2dm)) a
left join (select ptid, sum(n_basal)+sum(n_bolus) as n_fill_basal_bolus from ac_ehr_t2dm_trt_bas_bol where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus) group by ptid) b on a.ptid=b.ptid
left join (select ptid from ac_ehr_t2dm_trt_bas_bol_GLP where isnotnull(drug_flg)  group by ptid) c on a.ptid=c.ptid
inner join ac_ehr_obs_bmi_t2d_max_value_gt_30 d on a.ptid=d.ptid
where a.first_month_active_new<='2021-01-01' and '2021-12-31'<=a.last_month_active_new
;

select * from ac_ehr_t2dm_summary_bas_bol_GLP;

-- COMMAND ----------

drop table if exists ac_ehr_t2dm_wo_bmi_summary_bas_bol_GLP;

create table ac_ehr_t2dm_wo_bmi_summary_bas_bol_GLP as
select count(distinct a.ptid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.ptid) as n_basal_bolus, count(distinct c.ptid) as n_basal_bolus_GLP
, 100*count(distinct b.ptid)/count(distinct a.ptid) as pct_basal_bolus,
100*count(distinct c.ptid)/count(distinct b.ptid) as pct_basal_bolus_GLP
from (select ptid, dt_1st_dx_t2dm, first_month_active_new,last_month_active_new from ac_ehr_t2dm_trt_bas_bol_GLP where isnotnull(dt_1st_dx_t2dm)) a
left join (select ptid, sum(n_basal)+sum(n_bolus) as n_fill_basal_bolus from ac_ehr_t2dm_trt_bas_bol where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus) group by ptid) b on a.ptid=b.ptid
left join (select ptid from ac_ehr_t2dm_trt_bas_bol_GLP where isnotnull(drug_flg)  group by ptid) c on a.ptid=c.ptid
where a.first_month_active_new<='2021-01-01' and '2021-12-31'<=a.last_month_active_new
;

select * from ac_ehr_t2dm_wo_bmi_summary_bas_bol_GLP;
