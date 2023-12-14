-- Databricks notebook source
drop table if exists ac_dod_2303_lu_diag;

create table ac_dod_2303_lu_diag using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Lookup Diagnosis';

select * from ac_dod_2303_lu_diag;

-- COMMAND ----------

select distinct * from ac_dod_2303_lu_diag
where lower(diag_desc) like '%alcohol%'

-- COMMAND ----------

-- MAGIC %md #### Checking comorbidities

-- COMMAND ----------

-- drop table if exists ac_dod_dx_act_pancrtis_Test;

create or replace table ac_dod_dx_act_pancrtis_comorb as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                ,year(fst_dt) as yr_dt, b.yr_dt as year1
from ac_dod_2303_med_diag a
inner join ac_dod_act_pancrt_med_proc_001 b on a.patid=b.patid and year(a.fst_dt)= b.yr_dt
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_act_pancrtis_comorb
order by patid, fst_dt


-- COMMAND ----------

select distinct dx_name from ty00_all_dx_comorb

-- COMMAND ----------

select a.yr_dt, count(distinct a.patid) as pts from ac_dod_dx_act_pancrtis_comorb a
inner join ty00_all_dx_comorb b on a.diag=b.code
where b.dx_name='T1DM'
group by 1
order by 1
