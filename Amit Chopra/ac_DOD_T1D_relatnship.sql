-- Databricks notebook source

-- drop table if exists ac_dod_2304_med_diag;

-- create table ac_dod_2304_med_diag using delta location 'dbfs:/mnt/optumclin/202304/ontology/base/dod/Medical Diagnosis';

-- select * from ac_dod_2304_med_diag;
-- select max(fst_dt) from ac_dod_2304_med_diag;

-- drop table if exists ac_dod_2304_mem_enrol;

-- create table ac_dod_2304_mem_enrol using delta location 'dbfs:/mnt/optumclin/202304/ontology/base/dod/Member Enrollment';

-- select * from ac_dod_2304_mem_enrol;

-- drop table if exists ac_dod_2304_mem_cont_enrol;

-- create table ac_dod_2304_mem_cont_enrol using delta location 'dbfs:/mnt/optumclin/202304/ontology/base/dod/Member Continuous Enrollment';

-- select * from ac_dod_2304_mem_cont_enrol;

drop table if exists ac_dod_2304_HRA;

create table ac_dod_2304_HRA using delta location 'dbfs:/mnt/optumclin/202304/ontology/base/dod/HRA';

select * from ac_dod_2304_HRA;







-- COMMAND ----------

 select distinct * from ac_dod_2304_mem_enrol

-- COMMAND ----------

create or replace table ac_dx_t1d_2304 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
, b.Disease, b.dx_name, b.description, b.weight, b.weight_old 
from ac_dod_2304_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt>='2017-01-01' and b.dx_name='T1DM'
order by patid, fst_dt;  

select distinct * from ac_dx_t1d_2304
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dx_t1d_2304_2 as
select distinct a.*, c.eligeff, c.eligend, b.family_id, c.YRDOB, c.GDR_CD
from ac_dx_t1d_2304 a left join ac_dod_2304_mem_enrol b on a.patid=b.patid
left join ac_dod_2304_mem_cont_enrol c on a.patid=c.patid;

select distinct * from ac_dx_t1d_2304_2
order by patid, fst_dt;


-- COMMAND ----------

select family_id, count(distinct patid) from ac_dx_t1d_2304_2
group by 1
order by 2 desc;

-- COMMAND ----------

select distinct * from ac_dx_t1d_2304_2
where family_id='3073780475'

-- COMMAND ----------

select count(distinct patid) from ac_dx_t1d_2304_2
