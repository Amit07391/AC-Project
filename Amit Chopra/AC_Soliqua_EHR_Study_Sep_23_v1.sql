-- Databricks notebook source
create or replace table ac_ehr_sol_study_final_pts as
select distinct a.ptid, dt_rx_index, age_index, b.GENDER, b.ETHNICITY, b.DIVISION, b.race,b.region,b.birth_yr  from ac_ehr_sol_pat_16_23_all_enrol_2 a
left join ac_ehr_patient_202308_full_pts_2 b on a.ptid=b.PTID
where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and a.first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=a.last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30';

select distinct * from ac_ehr_sol_study_final_pts
order by ptid;

-- COMMAND ----------

drop table if exists ac_ehr_sol_lab_a1c_sol_tbl;

create table ac_ehr_sol_lab_a1c_sol_tbl as
select a.*,b.dt_rx_index, b.gender, b.ETHNICITY, b.DIVISION, date_add(b.dt_rx_index, 180) as fu_indx_dt, date_add(b.dt_rx_index, 30) as fu_30_indx_dt
from ac_ehr_lab_antibody_name_16_23_value a join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where isnotnull(b.dt_rx_index) and a.value between 5 and 15 
-- and a.service_date>=b.dt_rx_index - 180 and a.service_date <= b.dt_rx_index + 180
order by a.ptid, a.service_date
;

select distinct * from ac_ehr_sol_lab_a1c_sol_tbl
order by ptid, service_date;


-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_bl as
select distinct * from ac_ehr_sol_lab_a1c_sol_tbl
where service_date between dt_rx_index - 180 and dt_rx_index
order by ptid, service_date;

select distinct * from ac_ehr_sol_lab_a1c_sol_bl
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_bl_max as
select distinct ptid, max(service_date) as max_date from ac_ehr_sol_lab_a1c_sol_bl
group by 1
order by 1;

select distinct * from ac_ehr_sol_lab_a1c_sol_bl_max
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_bl_val as
select distinct ptid, max(value) as value1 from (
select distinct a.ptid, service_date, value from ac_ehr_sol_lab_a1c_sol_bl a
inner join ac_ehr_sol_lab_a1c_sol_bl_max b on a.ptid=b.ptid and a.service_date=b.max_date
order by 1,2) group by 1;

select distinct * from ac_ehr_sol_lab_a1c_sol_bl_val
order by 1,2;

-- COMMAND ----------

select distinct * from ac_ehr_sol_lab_a1c_sol_bl_val
where ptid='PT748349662'

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_fu as
select distinct * from ac_ehr_sol_lab_a1c_sol_tbl
where service_date between fu_indx_dt - 90 and fu_indx_dt + 30
order by ptid, service_date;

select distinct * from ac_ehr_sol_lab_a1c_sol_fu
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_fu_max as
select distinct ptid, max(service_date) as max_date from ac_ehr_sol_lab_a1c_sol_fu
group by 1
order by 1;

select distinct * from ac_ehr_sol_lab_a1c_sol_fu_max
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_fu_val as
select distinct ptid, max(value) as value1 from (
select distinct a.ptid, service_date, value from ac_ehr_sol_lab_a1c_sol_fu a
inner join ac_ehr_sol_lab_a1c_sol_fu_max b on a.ptid=b.ptid and a.service_date=b.max_date
order by 1,2) group by 1;

select distinct * from ac_ehr_sol_lab_a1c_sol_fu_val
order by 1;

-- COMMAND ----------

select distinct * from ac_ehr_sol_lab_a1c_sol_fu_val
where ptid='PT748349662'

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_bl_fl as
select distinct a.ptid, b.bl_hba1c_val, c.fl_hba1c_val
from ac_ehr_sol_lab_a1c_sol_tbl a
left join (select distinct ptid, value1 as bl_hba1c_val from ac_ehr_sol_lab_a1c_sol_bl_val
) b on a.ptid=b.ptid
left join (select distinct ptid, value1 as fl_hba1c_val from ac_ehr_sol_lab_a1c_sol_fu_val
) c on a.ptid=c.ptid
order by 1;

select distinct * from ac_ehr_sol_lab_a1c_sol_bl_fl
order by 1;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_lab_a1c_sol_fu_val
where value1<7

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_30dys_fu as
select distinct * from ac_ehr_sol_lab_a1c_sol_tbl
where service_date between fu_30_indx_dt - 29 and fu_30_indx_dt + 30
order by ptid, service_date;

select distinct * from ac_ehr_sol_lab_a1c_sol_30dys_fu
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_30dys_fu_max as
select distinct ptid, max(service_date) as max_date from ac_ehr_sol_lab_a1c_sol_30dys_fu
group by 1
order by 1;

select distinct * from ac_ehr_sol_lab_a1c_sol_30dys_fu_max
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_lab_a1c_sol_30dys_fu_val as
select distinct ptid, max(value) as value1 from (
select distinct a.ptid, service_date, value from ac_ehr_sol_lab_a1c_sol_30dys_fu a
inner join ac_ehr_sol_lab_a1c_sol_30dys_fu_max b on a.ptid=b.ptid and a.service_date=b.max_date
order by 1,2) group by 1;

select distinct * from ac_ehr_sol_lab_a1c_sol_30dys_fu_val
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Checking BMI and weight of the patients

-- COMMAND ----------

drop table if exists ac_ehr_sol_obs_sol_tbl;

create table ac_ehr_sol_obs_sol_tbl as
select a.*,b.dt_rx_index, b.GENDER, b.DIVISION, b.ETHNICITY, date_add(b.dt_rx_index, 180) as fu_indx_dt
from ac_ehr_obs_202308 a join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where isnotnull(b.dt_rx_index) and
a.OBS_TYPE in ('BMI', 'WT')
order by a.ptid, a.obs_date
;

select distinct * from ac_ehr_sol_obs_sol_tbl
order by ptid, obs_date;


-- COMMAND ----------

select distinct * from ac_ehr_sol_obs_sol_tbl 
where ptid='PT155436092'
and OBS_TYPE='WT'
order by ptid, obs_date;

-- COMMAND ----------

create or replace table ac_ehr_sol_WT_sol_bl as
select distinct * from ac_ehr_sol_obs_sol_tbl
where OBS_DATE between dt_rx_index - 180 and dt_rx_index and OBS_TYPE in ('WT')
order by ptid, OBS_DATE;

select distinct * from ac_ehr_sol_WT_sol_bl
order by ptid, OBS_DATE;

-- COMMAND ----------

select distinct obs_unit from ac_ehr_sol_WT_sol_bl

-- COMMAND ----------

create or replace table ac_ehr_sol_WT_sol_bl_max as
select distinct ptid, max(OBS_DATE) as max_date from ac_ehr_sol_WT_sol_bl
group by 1
order by 1;

select distinct * from ac_ehr_sol_WT_sol_bl_max
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_WT_sol_bl_val as
select distinct ptid, max(cast(OBS_RESULT as float)) as value1 from (
select distinct a.ptid, a.OBS_DATE, a.OBS_RESULT from ac_ehr_sol_WT_sol_bl a
inner join ac_ehr_sol_WT_sol_bl_max b on a.ptid=b.ptid and a.OBS_DATE=b.max_date
order by 1,2) group by 1;

select distinct * from ac_ehr_sol_WT_sol_bl_val
order by 1,2;

-- COMMAND ----------

create or replace table ac_ehr_sol_WT_sol_fu as
select distinct * from ac_ehr_sol_obs_sol_tbl
where OBS_DATE between fu_indx_dt - 90 and fu_indx_dt + 30 and OBS_TYPE in ('WT')
order by ptid, OBS_DATE;

select distinct * from ac_ehr_sol_WT_sol_fu
order by ptid, OBS_DATE;

-- COMMAND ----------

create or replace table ac_ehr_sol_WT_sol_fu_max as
select distinct ptid, max(OBS_DATE) as max_date from ac_ehr_sol_WT_sol_fu
group by 1
order by 1;

select distinct * from ac_ehr_sol_WT_sol_fu_max
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_WT_sol_fu_val as
select distinct ptid, max(cast(OBS_RESULT as float)) as value1 from (
select distinct a.ptid, a.OBS_DATE, a.OBS_RESULT from ac_ehr_sol_WT_sol_fu a
inner join ac_ehr_sol_WT_sol_fu_max b on a.ptid=b.ptid and a.OBS_DATE=b.max_date
order by 1,2) group by 1;

select distinct * from ac_ehr_sol_WT_sol_fu_val
order by 1,2;

-- COMMAND ----------

create or replace table ac_ehr_sol_WT_sol_bl_fu as
select distinct a.ptid, b.bl_wt_val, c.fl_wt_val
from ac_ehr_sol_obs_sol_tbl a
left join (select distinct ptid, value1 as bl_wt_val from ac_ehr_sol_WT_sol_bl_val
) b on a.ptid=b.ptid
left join (select distinct ptid, value1 as fl_wt_val from ac_ehr_sol_WT_sol_fu_val
) c on a.ptid=c.ptid
order by 1;

select distinct * from ac_ehr_sol_WT_sol_bl_fu
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Checking bl and fu BMI

-- COMMAND ----------

create or replace table ac_ehr_sol_BMI_sol_bl as
select distinct * from ac_ehr_sol_obs_sol_tbl
where OBS_DATE between dt_rx_index - 180 and dt_rx_index and OBS_TYPE in ('BMI')
order by ptid, OBS_DATE;

select distinct * from ac_ehr_sol_BMI_sol_bl
order by ptid, OBS_DATE;

-- COMMAND ----------

create or replace table ac_ehr_sol_BMI_sol_bl_max as
select distinct ptid, max(OBS_DATE) as max_date from ac_ehr_sol_BMI_sol_bl
group by 1
order by 1;

select distinct * from ac_ehr_sol_BMI_sol_bl_max
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_BMI_sol_bl_val as
select distinct ptid, max(cast(OBS_RESULT as float)) as value1 from (
select distinct a.ptid, a.OBS_DATE, a.OBS_RESULT from ac_ehr_sol_BMI_sol_bl a
inner join ac_ehr_sol_BMI_sol_bl_max b on a.ptid=b.ptid and a.OBS_DATE=b.max_date
order by 1,2) group by 1;

select distinct * from ac_ehr_sol_BMI_sol_bl_val
order by 1,2;

-- COMMAND ----------

create or replace table ac_ehr_sol_BMI_sol_fu as
select distinct * from ac_ehr_sol_obs_sol_tbl
where OBS_DATE between fu_indx_dt - 90 and fu_indx_dt + 30 and OBS_TYPE in ('BMI')
order by ptid, OBS_DATE;

select distinct * from ac_ehr_sol_BMI_sol_fu
order by ptid, OBS_DATE;


-- COMMAND ----------


create or replace table ac_ehr_sol_BMI_sol_fu_max as
select distinct ptid, max(OBS_DATE) as max_date from ac_ehr_sol_BMI_sol_fu
group by 1
order by 1;

select distinct * from ac_ehr_sol_BMI_sol_fu_max
order by 1;

-- COMMAND ----------


create or replace table ac_ehr_sol_BMI_sol_fu_val as
select distinct ptid, max(cast(OBS_RESULT as float)) as value1 from (
select distinct a.ptid, a.OBS_DATE, a.OBS_RESULT from ac_ehr_sol_BMI_sol_fu a
inner join ac_ehr_sol_BMI_sol_fu_max b on a.ptid=b.ptid and a.OBS_DATE=b.max_date
order by 1,2) group by 1;

select distinct * from ac_ehr_sol_BMI_sol_fu_val
order by 1,2;

-- COMMAND ----------

create or replace table ac_ehr_sol_BMI_sol_bl_fu as
select distinct a.ptid, b.bl_BMI_val, c.fl_BMI_val
from ac_ehr_sol_obs_sol_tbl a
left join (select distinct ptid, value1 as bl_BMI_val from ac_ehr_sol_BMI_sol_bl_val
) b on a.ptid=b.ptid
left join (select distinct ptid, value1 as fl_BMI_val from ac_ehr_sol_BMI_sol_fu_val
) c on a.ptid=c.ptid
order by 1;

select distinct * from ac_ehr_sol_BMI_sol_bl_fu
order by 1;

-- COMMAND ----------

select distinct * from ac_ehr_sol_BMI_sol_bl
where ptid='PT087147862'
