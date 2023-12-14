-- Databricks notebook source
-- drop table if exists ac_ehr_diag_202303;
-- CREATE TABLE ac_ehr_diag_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Diagnosis";

-- drop table if exists ac_mc_med_diag_202303;
-- CREATE TABLE ac_mc_med_diag_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Claims Medical Diagnosis";

-- select distinct * from ac_mc_med_diag_202303;


-- drop table if exists ac_mc_med_diag_202303;
-- CREATE TABLE ac_mc_med_diag_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Claims Medical Diagnosis";

select distinct * from ac_mc_med_diag_202303;


-- drop table if exists ac_mc_patient_202303;
-- CREATE TABLE ac_mc_patient_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/Patient";

-- select distinct * from ac_mc_patient_202303;




-- COMMAND ----------

select distinct code from ty00_all_dx_comorb
where dx_name='T2DM' 
order by 1;

-- select distinct code, dx_name from ty00_all_dx_comorb where dx_name in ('T1DM','T2DM')

-- COMMAND ----------

drop table if exists ac_ehr_dx_mc_indictn_13_23;
create or replace table ac_ehr_dx_mc_indictn_13_23 as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE
from ac_ehr_diag_202303 a 
where diag_date>='2013-01-01' and
((a.DIAGNOSIS_CD in ('D89811','D89810','E785','27952','27951','2724','42731') or a.DIAGNOSIS_CD like 'D8981%'
or a.DIAGNOSIS_CD like 'I48%' or a.DIAGNOSIS_CD like '2795%') or
diagnosis_cd in (select distinct code from ty00_all_dx_comorb where dx_name in ('T1DM','T2DM')))
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_mc_indictn_13_23
order by ptid, diag_date;

-- COMMAND ----------

create or replace table ac_ehr_dx_mc_indictn_13_23_2 as
select distinct a.*,
case when diagnosis_cd in ('D89811','27952') then 'cGVHD'
when diagnosis_cd in ('D89810','27951') then 'aGVHD'
when diagnosis_cd in ('E785','2724') then 'Dyslipidemia'
when diagnosis_cd like 'D8981%' or diagnosis_cd like '2795%' then 'GVHD'
when diagnosis_cd like 'I48%' or diagnosis_cd ='42731' then 'AFIB'
when diagnosis_cd in (select distinct code from ty00_all_dx_comorb where dx_name in ('T1DM')) then 'T1DM'
when diagnosis_cd in (select distinct code from ty00_all_dx_comorb where dx_name in ('T2DM')) then 'T2DM'
end as Diag_flag, b.market_clarity
from ac_ehr_dx_mc_indictn_13_23 a
left join ac_mc_patient_202303 b on a.ptid=b.ptid
where diagnosis_cd_type in ('ICD9','ICD10');


select distinct * from ac_ehr_dx_mc_indictn_13_23_2
order by ptid, diag_date;

-- COMMAND ----------

-- select distinct diagnosis_cd from ac_ehr_dx_mc_indictn_13_23_2
-- where diag_flag='GVHD';

select max(diag_date) from ac_ehr_dx_mc_indictn_13_23_2

-- COMMAND ----------

select year(diag_date) as yr, Diag_flag, count(distinct ptid) from ac_ehr_dx_mc_indictn_13_23_2
-- where market_clarity='Y'
group by 1,2
order by 1,2;

-- select Diag_flag, count(distinct ptid) from ac_ehr_dx_mc_indictn_13_23_2
-- -- where market_clarity='Y'
-- group by 1
-- order by 1;

-- COMMAND ----------

-- MAGIC %md #### Using claims table of market clarity

-- COMMAND ----------

drop table if exists ac_clms_dx_mc_indictn_13_23;
create or replace table ac_clms_dx_mc_indictn_13_23 as
select distinct a.*
from ac_mc_med_diag_202303 a 
where fst_dt>='2013-01-01' and
((a.diag in ('D89811','D89810','E785','27952','27951','2724','42731') or a.diag like 'D8981%'
or a.diag like 'I48%' or a.diag like '2795%') or
diag in (select distinct code from ty00_all_dx_comorb where dx_name in ('T1DM','T2DM')))
;

select distinct * from ac_clms_dx_mc_indictn_13_23
order by ptid, fst_dt;

-- COMMAND ----------

create or replace table ac_clms_dx_mc_indictn_13_23_2 as
select distinct *,
case when diag in ('D89811','27952') then 'cGVHD'
when diag in ('D89810','27951') then 'aGVHD'
when diag in ('E785','2724') then 'Dyslipidemia'
when diag like 'D8981%' or diag like '2795%' then 'GVHD'
when diag like 'I48%' or diag ='42731' then 'AFIB'
when diag in (select distinct code from ty00_all_dx_comorb where dx_name in ('T1DM')) then 'T1DM'
when diag in (select distinct code from ty00_all_dx_comorb where dx_name in ('T2DM')) then 'T2DM'
end as Diag_flag from ac_clms_dx_mc_indictn_13_23
order by ptid, fst_dt;

select distinct * from ac_clms_dx_mc_indictn_13_23_2
order by ptid, fst_dt;

-- COMMAND ----------

select max(fst_dt) from ac_clms_dx_mc_indictn_13_23_2

-- COMMAND ----------

select year(fst_dt) as yr, Diag_flag, count(distinct ptid) from ac_clms_dx_mc_indictn_13_23_2
group by 1,2
order by 1,2;

select Diag_flag, count(distinct ptid) from ac_clms_dx_mc_indictn_13_23_2
group by 1
order by 1;
