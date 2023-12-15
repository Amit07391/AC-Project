-- Databricks notebook source
-- drop table if exists ac_dod_2305_med_diag;

-- CREATE TABLE ac_dod_2305_med_diag USING DELTA LOCATION 'dbfs:/mnt/optumclin/202305/ontology/base/dod/Medical Diagnosis';

-- select * from ac_dod_2305_med_diag;

-- drop table if exists ac_dod_2305_rx_claims;

-- CREATE TABLE ac_dod_2305_rx_claims USING DELTA LOCATION 'dbfs:/mnt/optumclin/202305/ontology/base/dod/RX Claims';

-- select * from ac_dod_2305_rx_claims;

drop table if exists ac_dod_2305_labs;

CREATE TABLE ac_dod_2305_labs USING DELTA LOCATION 'dbfs:/mnt/optumclin/202305/ontology/base/dod/Lab Results';

select * from ac_dod_2305_labs;

-- COMMAND ----------

select max(fst_dt) from ac_dod_2305_med_diag

-- COMMAND ----------

create or replace table ac_dod_dx_t2d_sol as 
select distinct a.*, b.dx_name from ac_dod_2305_med_diag a 
inner join ty00_all_dx_comorb b on a.DIAG=b.code
where a.fst_dt>='2016-07-01' AND a.fst_dt<='2022-06-30' AND
b.dx_name='T2DM'
order by a.patid, a.FST_DT;

select distinct * from ac_dod_dx_t2d_sol
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_dx_t2d_sol_diag_cnt as
select distinct patid, count(distinct fst_dt) as diag_cnt 
from ac_dod_dx_t2d_sol
group by 1
order by 1;

select distinct * from ac_dod_dx_t2d_sol_diag_cnt
order by 1;

-- COMMAND ----------

create or replace table ac_dod_dx_t2d_sol_1 as
select distinct a.patid, a.fst_dt, lead(fst_dt) over (partition by a.patid order by fst_dt) as next_dt, dense_rank() OVER (PARTITION BY a.patid ORDER BY fst_dt) as rank, b.diag_cnt
from ac_dod_dx_t2d_sol a
inner join ac_dod_dx_t2d_sol_diag_cnt b on a.patid=b.patid
order by 1,2;

select distinct * from ac_dod_dx_t2d_sol_1
order by 1,2;

-- COMMAND ----------

create or replace table ac_dod_dx_t2d_sol_2 as
select distinct *, date_diff(next_dt, fst_dt) as diff from ac_dod_dx_t2d_sol_1
where diag_cnt>=2
order by 1;

select distinct * from ac_dod_dx_t2d_sol_2
order by 1,2;


-- COMMAND ----------


create or replace table ac_dod_dx_t2d_sol_3 as
select distinct * from ac_dod_dx_t2d_sol_2
where diff>=30
order by 1,2;

select distinct * from ac_dod_dx_t2d_sol_3
order by 1,2;

-- COMMAND ----------

select count(distinct patid) from ac_dod_dx_t2d_sol_3

-- COMMAND ----------

create or replace table ac_dod_dx_t2d_sol_4 as
select distinct a.* from ac_dod_dx_t2d_sol a
inner join ac_dod_dx_t2d_sol_3 b on a.patid=b.patid
order by patid, fst_dt;

select count(distinct patid) from ac_dod_dx_t2d_sol_4;


-- COMMAND ----------

create or replace table ac_dod_dx_t1d_secnd_sol as 
select distinct a.* from ac_dod_2305_med_diag a 
where a.fst_dt>='2016-07-01' AND a.fst_dt<='2022-06-30' AND
(a.DIAG in (select code from ty00_all_dx_comorb where dx_name='T1DM')
or DIAG like '249%' or DIAG like 'E08%' or DIAG like 'E09%')
order by a.patid, a.FST_DT;

select distinct * from ac_dod_dx_t1d_secnd_sol
order by patid, fst_dt;

-- COMMAND ----------

create table ac_2305_rx_anti_dm as
select distinct a.PATID, a.PAT_PLANID, a.AVGWHLSL, a.CHARGE, a.CLMID, a.COPAY, a.DAW, a.DAYS_SUP, a.DEDUCT, a.DISPFEE, a.FILL_DT, a.MAIL_IND, a.NPI, a.PRC_TYP, a.QUANTITY, a.RFL_NBR, a.SPECCLSS, a.STD_COST, a.STD_COST_YR, a.STRENGTH, b.*
from ac_dod_2305_rx_claims a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt;

select distinct * from ac_2305_rx_anti_dm
order by patid, fill_dt;

-- COMMAND ----------

select distinct rx_type from ac_2305_rx_anti_dm
where lower(BRND_NM) like '%soliqua%'

-- COMMAND ----------

create or replace table ac_pat_rx_bas_bol_sol as
select distinct patid, charge,clmid,copay,days_sup,deduct,dispfee,fill_dt,quantity,specclss,std_cost,std_cost_yr,strength,brnd_nm,gnrc_nm,ndc,rx_type
from ac_2305_rx_anti_dm
where fill_dt>='2016-01-01' AND fill_dt<='2022-06-30' and lcase(rx_type) in ('basal', 'bolus')
order by patid, fill_dt;

select distinct * from ac_pat_rx_bas_bol_sol
order by patid, fill_dt;

-- COMMAND ----------

drop table if exists ac_pat_dx_rx_sol;

create table ac_pat_dx_rx_sol as
select distinct a.patid,min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_Basal) as dt_1st_Basal, min(d.dt_1st_Bolus) as dt_1st_Bolus
from (select distinct patid, min(fst_dt) as dt_1st_t2dm, count(distinct fst_dt) as n_t2dm from ac_dod_dx_t2d_sol_4 group by patid) a
      left join (select distinct patid, min(fst_dt) as dt_1st_t1dm, count(distinct fst_dt) as n_t1dm from ac_dod_dx_t1d_secnd_sol group by patid) b on a.patid=b.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_Basal from ac_pat_rx_bas_bol_sol WHERE rx_type in ('Basal') and fill_dt>='2016-07-01' AND fill_dt<='2022-06-30' group by patid) c on a.patid=c.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_Bolus from ac_pat_rx_bas_bol_sol WHERE rx_type in ('Bolus') and fill_dt>='2016-07-01' AND fill_dt<='2022-06-30' group by patid) d on a.patid=d.patid
group by a.patid
order by a.patid
;

select distinct * from ac_pat_dx_rx_sol
order by 1;

-- COMMAND ----------

select max(fst_dt) from ty19_lab_a1c_loinc_value

-- COMMAND ----------

create or replace table ac_lab_2305_a1c_loinc as
select distinct *, cast(rslt_txt as double) as result
from ac_dod_2305_labs
where lcase(loinc_cd) in ('17855-8', '17856-6','41995-2','4548-4','45484','4637-5','55454-3','hgba1c') or (isnull(loinc_cd) and lcase(tst_desc) in ('a1c','glyco hemoglobin a1c','glycohemoglobin (a1c) glycohem','glycohemoglobin a1c','hemoglobin a1c','hgb a1c','hba1c','hemoglob a1c','hemoglobin a1c'
,'hemoglobin a1c w/o eag','hgb-a1c','hgba1c-t'))
order by patid, fst_dt;
select * from ac_lab_2305_a1c_loinc;
select loinc_cd, tst_desc, count(*) as n_obs from ac_lab_2305_a1c_loinc
--where lcase(tst_desc) like '%a1c%'
group by loinc_cd, tst_desc
order by loinc_cd, tst_desc
; select RSLT_TXT, count(*) as n_obs from ac_lab_2305_a1c_loinc
--where lcase(tst_desc) like '%a1c%'
group by RSLT_TXT
order by RSLT_TXT;
create or replace table ac_lab_2305_a1c_loinc_value as
select distinct *, 
case when rslt_nbr>0 then rslt_nbr
when rslt_nbr=0 and isnotnull(result) then result
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%' or rslt_txt like '%=%' or substr(rslt_txt,-1)='%') then cast(rslt_txt as double)
when rslt_nbr=0 and isnull(result) and rslt_txt like '>%' and not(rslt_txt like '>=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '>=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<%' and not(rslt_txt like '<=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)-0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)-0.1
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%') and substr(rslt_txt,-1)='%' then cast(substring_index(rslt_txt,'%',1) as double)
else null end value
from ac_lab_2305_a1c_loinc
; 
select distinct * from ac_lab_2305_a1c_loinc_value where
isnotnull(value)
;

-- COMMAND ----------

-- drop table if exists ac_2305_lab_a1c_sol_1;

-- create table ac_2305_lab_a1c_sol_1 as
-- select a.*
-- from ac_lab_2305_a1c_loinc_value a join ac_pat_dx_rx_sol b
-- on a.patid=b.patid
-- where a.value between 5 and 15
-- order by a.patid, a.fst_dt
-- ;

-- select * from ac_2305_lab_a1c_sol_1;

create or replace table ac_2305_lab_a1c_sol_2 as
select distinct * from ac_2305_lab_a1c_sol_1
where value>=8 and fst_dt>='2017-01-01' AND fst_dt<='2021-12-31'
order by patid, fst_dt;

select * from ac_2305_lab_a1c_sol_2
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_2305_lab_a1c_sol_index as
select distinct patid, min(fst_dt) as indx_a1c_dt 
from ac_2305_lab_a1c_sol_2 
group by 1
order by 1;


select distinct * from ac_2305_lab_a1c_sol_index
order by 1;

create or replace table ac_2305_lab_a1c_sol_3 as
select distinct a.*, b.indx_a1c_dt 
from ac_2305_lab_a1c_sol_2 a
inner join ac_2305_lab_a1c_sol_index b on a.patid=b.patid
order by a.patid, a.fst_dt;

select distinct * from ac_2305_lab_a1c_sol_3
order by patid, fst_dt;


-- COMMAND ----------

-- MAGIC %md #### Checking baseline bolus and basal use

-- COMMAND ----------

create or replace table ac1206_bas_bol_bl_use as
select distinct a.*, dateadd(fill_dt, cast(days_sup as int)) as end_date, b.indx_a1c_dt
from ac_pat_rx_bas_bol_sol a 
inner join ac_2305_lab_a1c_sol_3 b on a.patid=b.patid
where a.fill_dt between b.indx_a1c_dt - 180 and b.indx_a1c_dt - 1
order by a.patid, a.fill_dt;

select distinct * from ac1206_bas_bol_bl_use
order by patid, fill_dt;

-- COMMAND ----------

select distinct * from ac1206_bas_bol_bl_use
where patid='33003288149'
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac1206_bas_bol_bl_use_1 as
select distinct a.* from ac1206_bas_bol_bl_use a 
join ac1206_bas_bol_bl_use b on a.patid=b.patid and a.rx_type<>b.rx_type
where ((date_diff(b.end_date,a.fill_dt)>=30 ) AND
(date_diff(a.end_date,b.fill_dt)>=30 ))
order by a.patid, a.fill_dt;

select distinct * from ac1206_bas_bol_bl_use_1
order by patid, fill_dt;

-- COMMAND ----------

-- MAGIC %md #### COmbination of basal and bolus

-- COMMAND ----------

create or replace table ac1206_bas_bol_comb as
select distinct patid, gnrc_nm, brnd_nm from ac1206_bas_bol_bl_use_1
order by 1;

select distinct * from ac1206_bas_bol_comb
order by 1;

-- COMMAND ----------

create or replace table ac1206_bas_bol_comb_2 as
select distinct patid, concat_ws('--', collect_list(gnrc_nm)) as gnrc_nm from ac1206_bas_bol_comb
group by 1
order by 1;

create or replace table ac1206_bas_bol_comb_2 as
select distinct patid, concat_ws('--', collect_list(brnd_nm)) as brnd_nm from ac1206_bas_bol_comb
group by 1
order by 1;

-- COMMAND ----------

select distinct brnd_nm, count(distinct patid) from ac1206_bas_bol_comb_2
group by 1
order by 2 desc

-- COMMAND ----------

select distinct * from ac1206_bas_bol_bl_use_1
where patid='33003287123'
order by patid, fill_dt;

select distinct patid, count(distinct rx_type) from ac1206_bas_bol_bl_use_1
group by 1
order by 2 asc;

-- COMMAND ----------

drop table if exists ac_pat_rx_basal_bolus_index;

create table ac_pat_rx_basal_bolus_index as
select distinct a.*, b.indx_a1c_dt
from ac_pat_rx_bas_bol_sol a inner join ac_2305_lab_a1c_sol_index b
on a.patid=b.patid
order by a.patid, a.fill_dt
;

select * from ac_pat_rx_basal_bolus_index;

-- COMMAND ----------

drop table if exists ac_bas_bol_sol_bl;

create table ac_bas_bol_sol_bl as
select distinct a.patid as patid1, max(b.dt_last_bas_bol_bl) as dt_last_bas_bol_bl, max(c.dt_last_sol_bl) as dt_last_sol_bl
from (select patid, indx_a1c_dt from ac_2305_lab_a1c_sol_3 ) a
     left join (select distinct patid, max(fill_dt) as dt_last_bas_bol_bl from ac1206_bas_bol_bl_use_1 group by patid) b on a.patid=b.patid
     left join (select distinct patid, max(fill_dt) as dt_last_sol_bl from ac_pat_rx_basal_bolus_index where (fill_dt between date_sub(indx_a1c_dt,180) and date_sub(indx_a1c_dt,1)) and lower(BRND_NM) like '%soliqua%' group by patid) c on a.patid=c.patid
group by patid1
order by patid1
;

select * from ac_bas_bol_sol_bl;

-- COMMAND ----------

create or replace table pat_all_sol as
select distinct a.*, b.* from ac_pat_dx_rx_sol a
left join ac_bas_bol_sol_bl b on a.patid=b.patid1;

select distinct * from pat_all_sol
-- where isnotnull(dt_last_sol_bl)
order by patid;

-- COMMAND ----------

drop table if exists ac_patient_attrition_sol;

create table ac_patient_attrition_sol as
select ' 1. At least two diagnoses of T2D (30 days apart) during the study period' as Step, count(distinct patid) as n_pat from pat_all_sol where isnotnull(dt_1st_t2dm)
union
select ' 2. Include patients with at least one basal insulin prescription and one bolus insulin prescription during the study period' as Step, count(distinct patid) as n_pat from pat_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus)
union
select ' 3. Include patients with one or more valid HbA1c measurment and HbA1c >=8% during the study period.' as Step, count(distinct patid) as n_pat from pat_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(patid1)
union
select ' 4. Include patients with at least 30 days overlap between basal insulin and bolus insulin (BB) during the baseline period (6 months prior to the index date)' as Step, count(distinct patid) as n_pat from pat_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(patid1) and isnotnull(dt_last_bas_bol_bl)
union
select ' 5. Exclude (1) Diagnoses of T1D at any time during the study period (2) Diagnosis of secondary diabetes during the study period (3) with iGlarLixi prescriptions during the baseline period' as Step, count(distinct patid) as n_pat from pat_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(patid1) and isnotnull(dt_last_bas_bol_bl) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_sol_bl)
order by Step
;

select * from ac_patient_attrition_sol;
