-- Databricks notebook source


-- drop table if exists ac_dod_2307_med_diag;
-- create table ac_dod_2307_med_diag using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Medical Diagnosis';

-- select distinct * from ac_dod_2307_med_diag;


-- drop table if exists ac_dod_2307_lu_ndc;
-- create table ac_dod_2307_lu_ndc using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Lookup NDC';

-- select distinct * from ac_dod_2307_lu_ndc;

-- drop table if exists ac_dod_2305_member_enrol;
-- create table ac_dod_2305_member_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Enrollment';

-- select distinct * from ac_dod_2305_member_enrol;

-- drop table if exists ac_dod_2307_member_cont_enrol;
-- create table ac_dod_2307_member_cont_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Continuous Enrollment';

select distinct * from ac_dod_2307_member_cont_enrol;

-- drop table if exists ac_dod_2307_rx_claims;
-- create table ac_dod_2307_rx_claims using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/RX Claims';

-- select distinct * from ac_dod_2307_rx_claims;

-- drop table if exists ac_dod_2307_med_claims;
-- create table ac_dod_2307_med_claims using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Medical Claims';

-- select distinct * from ac_dod_2307_med_claims;

-- drop table if exists ac_dod_2307_lu_proc;
-- create table ac_dod_2307_lu_proc using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Lookup Procedure';

-- select distinct * from ac_dod_2307_lu_proc;

-- drop table if exists ac_dod_2307_lu_diag;
-- create table ac_dod_2307_lu_diag using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Lookup Diagnosis';

-- select distinct * from ac_dod_2307_lu_diag;


-- drop table if exists ac_dod_2307_labs;

-- CREATE TABLE ac_dod_2307_labs USING DELTA LOCATION 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Lab Results';

-- select * from ac_dod_2307_labs;




-- COMMAND ----------

create or replace table ac_dod_dx_t2dm_16_22 as 
select distinct a.*, b.dx_name from ac_dod_2307_med_diag a 
inner join ty00_all_dx_comorb b on a.DIAG=b.code
where a.fst_dt>='2016-07-01' AND a.fst_dt<='2022-06-30' AND
b.dx_name='T2DM'
order by a.patid, a.FST_DT;

select distinct * from ac_dod_dx_t2dm_16_22
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_dx_t2dm_16_22 -- 5733806

-- COMMAND ----------

create or replace table ac_dod_dx_t1d_secnd_16_22 as 
select distinct a.* from ac_dod_2307_med_diag a 
where a.fst_dt>='2016-07-01' AND a.fst_dt<='2022-06-30' AND
(a.DIAG in (select code from ty00_all_dx_comorb where dx_name='T1DM')
or DIAG like '249%' or DIAG like 'E08%' or DIAG like 'E09%')
order by a.patid, a.FST_DT;

select distinct * from ac_dod_dx_t1d_secnd_16_22
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_dx_t1d_secnd_16_22 -- 5733806

-- COMMAND ----------

create table ac_2307_rx_anti_dm as
select distinct a.PATID, a.PAT_PLANID, a.AVGWHLSL, a.CHARGE, a.CLMID, a.COPAY, a.DAW, a.DAYS_SUP, a.DEDUCT, a.DISPFEE, a.FILL_DT, a.MAIL_IND, a.NPI, a.PRC_TYP, a.QUANTITY, a.RFL_NBR, a.SPECCLSS, a.STD_COST, a.STD_COST_YR, a.STRENGTH, b.*
from ac_dod_2307_rx_claims a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt;

select distinct * from ac_2307_rx_anti_dm
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dod_pat_rx_bas_bol_sol as
select distinct * from ac_2307_rx_anti_dm
where FILL_DT>='2015-01-01'
order by patid, FILL_DT;

select distinct * from ac_dod_pat_rx_bas_bol_sol

order by patid, FILL_DT;


-- COMMAND ----------

drop table if exists ac_dod_pat_dx_rx_sol;

create table ac_dod_pat_dx_rx_sol as
select distinct a.patid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_soliqua) as dt_rx_index, min(d.dt_1st_sec_diab) as dt_1st_sec_diab
from (select distinct patid, min(FST_DT) as dt_1st_t2dm, count(distinct FST_DT) as n_t2dm from ac_dod_dx_t2dm_16_22
where FST_DT between '2016-07-01' and '2022-06-30' group by patid) a
      left join (select distinct patid, min(FST_DT) as dt_1st_t1dm, count(distinct FST_DT) as n_t1dm from ac_dod_dx_t1d_secnd_16_22 where FST_DT between '2016-07-01' and '2022-06-30' and DIAG in (select code from ty00_all_dx_comorb where dx_name='T1DM') group by patid) b on a.patid=b.patid
       left join (select distinct patid, min(FST_DT) as dt_1st_sec_diab from ac_dod_dx_t1d_secnd_16_22 where FST_DT between '2016-07-01' and '2022-06-30' and (DIAG like '249%' or DIAG like 'E08%' or DIAG like 'E09%') group by patid) d on a.patid=d.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_soliqua from ac_dod_pat_rx_bas_bol_sol where fill_dt between '2017-01-01' and  '2021-12-31' and lower(brnd_nm) like ('%soliqua%') group by patid) c on a.patid=c.patid
group by a.patid
order by a.patid
;

select distinct *
from ac_dod_pat_dx_rx_sol
where isnotnull(dt_rx_index)
;


-- COMMAND ----------

drop table if exists ac_dod_pat_rx_bas_bol_sol_index;

create table ac_dod_pat_rx_bas_bol_sol_index as
select distinct a.*, b.dt_rx_index
from ac_dod_pat_rx_bas_bol_sol a left join ac_dod_pat_dx_rx_sol b
on a.patid=b.patid
order by a.patid, a.fill_dt
;

select * from ac_dod_pat_rx_bas_bol_sol_index
order by patid, fill_dt;


-- COMMAND ----------

create or replace table ac_lab_2307_a1c_loinc as
select distinct *, cast(rslt_txt as double) as result
from ac_dod_2307_labs
where lcase(loinc_cd) in ('17855-8', '17856-6','41995-2','4548-4','45484','4637-5','55454-3','hgba1c') or (isnull(loinc_cd) and lcase(tst_desc) in ('a1c','glyco hemoglobin a1c','glycohemoglobin (a1c) glycohem','glycohemoglobin a1c','hemoglobin a1c','hgb a1c','hba1c','hemoglob a1c','hemoglobin a1c'
,'hemoglobin a1c w/o eag','hgb-a1c','hgba1c-t'))
order by patid, fst_dt;
select * from ac_lab_2307_a1c_loinc;


create or replace table ac_lab_2307_a1c_loinc_value as
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
from ac_lab_2307_a1c_loinc;

select distinct * from ac_lab_2307_a1c_loinc_value where
isnotnull(value);

-- COMMAND ----------

drop table if exists ac_dod_2307_lab_a1c_sol_index;

create table ac_dod_2307_lab_a1c_sol_index as
select a.*,b.dt_rx_index
from ac_lab_2307_a1c_loinc_value a join ac_dod_pat_dx_rx_sol b
on a.patid=b.patid
where isnotnull(b.dt_rx_index) and a.value between 5 and 15
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_2307_lab_a1c_sol_index
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_pat_rx_bas_bol_index_only as
select distinct *, dateadd(fill_dt, cast(days_sup as int)) as end_date from ac_dod_pat_rx_bas_bol_sol_index
where lcase(rx_type) in ('basal', 'bolus') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1)
order by patid, fill_dt;

select distinct * from ac_dod_pat_rx_bas_bol_index_only
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_rx_anti_dm_bas_bol_overlap as
select distinct a.* from ac_dod_pat_rx_bas_bol_index_only a 
join ac_dod_pat_rx_bas_bol_index_only b on a.patid=b.patid and a.rx_type<>b.rx_type
where ((a.FILL_DT between b.FILL_DT and b.end_date + 30 ) OR
(b.FILL_DT between a.FILL_DT and a.end_date + 30 ))
order by a.patid, a.fill_dt;

select distinct * from ac_rx_anti_dm_bas_bol_overlap
order by patid, fill_dt;

-- COMMAND ----------

select distinct * from ac_dod_pat_rx_bas_bol_index_only
where patid='33004234178'
order by fill_dt;


-- COMMAND ----------

drop table if exists ac_dod_sol_a1c_bl_fu;

create table ac_dod_sol_a1c_bl_fu as 
select distinct a.patid as patid1, max(b.dt_last_bas_bl) as dt_last_bas_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, max(e.dt_last_sol_bl) as dt_last_sol_bl, max(f.dt_last_bolus_bl) as dt_last_bolus_bl, min(g.dt_1st_bas_bol_fu) as dt_1st_bas_bol_fu, min(h.dt_1st_bol_fu) as dt_1st_bol_fu, max(i.dt_last_bas_bol_ovrlp_bl) as dt_last_bas_bol_ovrlp_bl
from (select patid, dt_rx_index from ac_dod_pat_dx_rx_sol where isnotnull(dt_rx_index)) a
     left join (select distinct patid, max(FILL_DT) as dt_last_bas_bl from ac_dod_pat_rx_bas_bol_sol_index where lcase(rx_type) in ('basal') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) b on a.patid=b.patid
     left join (select distinct patid, max(FST_DT) as dt_last_a1c_bl from ac_dod_2307_lab_a1c_sol_index where FST_DT between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) c on a.patid=c.patid
     left join (select distinct patid, min(FST_DT) as dt_1st_a1c_fu from ac_dod_2307_lab_a1c_sol_index where FST_DT between dt_rx_index + 90 and dt_rx_index + 210  group by patid) d on a.patid=d.patid
     left join (select distinct patid, max(fill_dt) as dt_last_sol_bl from ac_dod_pat_rx_bas_bol_sol_index where lcase(brnd_nm) like ('%soliqua%') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) e on a.patid=e.patid
     left join (select distinct patid, max(fill_dt) as dt_last_bolus_bl from ac_dod_pat_rx_bas_bol_sol_index where lcase(rx_type)  in ('bolus') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) f on a.patid=f.patid
     left join (select distinct patid, min(fill_dt) as dt_1st_bas_bol_fu from ac_dod_pat_rx_bas_bol_sol_index where lcase(rx_type)  in ('basal','bolus') and fill_dt between dt_rx_index + 1 and dt_rx_index + 180 group by patid) g on a.patid=g.patid
     left join (select distinct patid, min(fill_dt) as dt_1st_bol_fu from ac_dod_pat_rx_bas_bol_sol_index where lcase(rx_type)  in ('bolus') and fill_dt between dt_rx_index + 1 and dt_rx_index + 180 group by patid) h on a.patid=h.patid
     left join (select distinct patid, max(fill_dt) as dt_last_bas_bol_ovrlp_bl from ac_rx_anti_dm_bas_bol_overlap group by patid) i on a.patid=i.patid
group by patid1
order by patid1
;

select * from ac_dod_sol_a1c_bl_fu;

-- COMMAND ----------

drop table if exists ac_dod_sol_pat_all_enrol;

create table ac_dod_sol_pat_all_enrol as
select distinct a.*,
b.*, c.ELIGEFF , c.ELIGEND, c.GDR_CD, c.YRDOB, year(a.dt_rx_index) as yr_indx
from ac_dod_pat_dx_rx_sol a left join ac_dod_sol_a1c_bl_fu b on a.patid=b.patid1
                      left join ac_dod_2307_member_cont_enrol c on a.patid=c.patid and a.dt_rx_index between c.ELIGEFF and c.ELIGEND
order by a.patid
;

select * from ac_dod_sol_pat_all_enrol;

create or replace table ac_dod_sol_pat_all_enrol_2 as
select distinct * , yr_indx - yrdob as Age_index from ac_dod_sol_pat_all_enrol
order by patid;

select * from ac_dod_sol_pat_all_enrol_2;

-- COMMAND ----------

drop table if exists ac_dod_patient_attrition_soliqua;

create or replace table ac_dod_patient_attrition_soliqua as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2016 and 06/30/2022' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2 _2 where dt_1st_t2dm between '2016-07-01' and '2022-06-30'
union
select ' 2. Had at least one iGlarLixi prescription during the identification (ID) period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where  isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 5a. Had at least one basal insulin prescription during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 5b. (5b) Had at least one bolus insulin prescription during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 6. Had one or more valid HbA1c measurement(s) during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 7. Had one or more valid HbA1c measurement(s) between 90- and 210-days post-index date' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 8. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 9. Those without any secondary diabetes diagnoses identified' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 9b. Had iGlarLixi prescriptions during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 9c. Had basal or bolus prescriptions during the follow-up period (180 days post-index date)' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and isnull(dt_1st_bas_bol_fu) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
-- union
-- select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ac_pat_all_enrol_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ac_dod_patient_attrition_soliqua ;

-- COMMAND ----------

drop table if exists ac_dod_patient_attrition_soliqua_overlap;

create or replace table ac_dod_patient_attrition_soliqua_overlap as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2016 and 06/30/2022' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2 _2 where dt_1st_t2dm between '2016-07-01' and '2022-06-30'
union
select ' 2. Had at least one iGlarLixi prescription during the identification (ID) period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where  isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 5a. Had at least one basal insulin prescription during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 5b. (5b) Had at least one bolus insulin prescription during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 5c. (5c) Had at least 30 days overlap between basal insulin and bolus insulin during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_bas_bol_ovrlp_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 6. Had one or more valid HbA1c measurement(s) during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_bas_bol_ovrlp_bl) and isnotnull(dt_last_a1c_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 7. Had one or more valid HbA1c measurement(s) between 90- and 210-days post-index date' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_bas_bol_ovrlp_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 8. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_bas_bol_ovrlp_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 9. Those without any secondary diabetes diagnoses identified' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_bas_bol_ovrlp_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 9b. Had iGlarLixi prescriptions during the baseline period' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_bas_bol_ovrlp_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
union
select ' 9c. Had basal or bolus prescriptions during the follow-up period (180 days post-index date)' as Step, count(distinct patid) as n_pat from ac_dod_sol_pat_all_enrol_2  where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and eligeff<=dt_rx_index - 180  and dt_rx_index<=ELIGEND and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_bas_bol_ovrlp_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and isnull(dt_1st_bas_bol_fu) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'
-- union
-- select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ac_pat_all_enrol_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ac_dod_patient_attrition_soliqua_overlap ;
