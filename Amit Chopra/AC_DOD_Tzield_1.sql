-- Databricks notebook source


-- drop table if exists ac_dod_2307_med_diag;
-- create table ac_dod_2307_med_diag using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Medical Diagnosis';

-- select distinct * from ac_dod_2307_med_diag;


drop table if exists ac_dod_2307_lu_ndc;
create table ac_dod_2307_lu_ndc using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Lookup NDC';

select distinct * from ac_dod_2307_lu_ndc;

-- drop table if exists ac_dod_2305_member_enrol;
-- create table ac_dod_2305_member_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Enrollment';

-- select distinct * from ac_dod_2305_member_enrol;

-- drop table if exists ac_dod_2305_member_cont_enrol;
-- create table ac_dod_2305_member_cont_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Continuous Enrollment';

-- select distinct * from ac_dod_2305_member_cont_enrol;

-- drop table if exists ac_dod_2307_rx_claims;
-- create table ac_dod_2307_rx_claims using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/RX Claims';

-- select distinct * from ac_dod_2307_rx_claims;

drop table if exists ac_dod_2307_med_claims;
create table ac_dod_2307_med_claims using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Medical Claims';

select distinct * from ac_dod_2307_med_claims;

-- drop table if exists ac_dod_2305_lu_proc;
-- create table ac_dod_2305_lu_proc using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Lookup Procedure';

-- select distinct * from ac_dod_2305_lu_proc;





-- COMMAND ----------

select distinct * from ac_dod_2305_lu_ndc

-- COMMAND ----------

select distinct * from ac_dod_2307_lu_ndc
where upper(PROC_DESC) like '%TZIELD%' or upper(PROC_DESC) like '%TEPLIZUMAB%'

-- COMMAND ----------

create or replace table ac_DOD_tzd_lu_ndc as
select distinct * from ac_dod_2307_lu_ndc
where upper(BRND_NM) like '%TZIELD%' or upper(GNRC_NM) like '%TZIELD%' or
upper(BRND_NM) like '%TEPLIZUMAB%' or upper(GNRC_NM) like '%TEPLIZUMAB%';
;

select distinct * from ac_DOD_tzd_lu_ndc

-- COMMAND ----------

describe table ac_dod_2305_rx_claims

-- COMMAND ----------

create or replace table ac_dod_TZD_rx_claims as
select distinct * from ac_dod_2307_rx_claims 
where NDC in ('73650031601','73650031614','73650031610') or upper(BRND_NM) like '%TZIELD%' or upper(GNRC_NM) like '%TZIELD%' or
upper(BRND_NM) like '%TEPLIZUMAB%' or upper(GNRC_NM) like '%TEPLIZUMAB%'
order by patid, FILL_DT;

select distinct * from ac_dod_TZD_rx_claims
order by patid, FILL_DT;

-- COMMAND ----------

create or replace table ac_dod_TZD_med_claims as 
select distinct * from ac_dod_2307_med_claims
where proc_CD in ('C9149','J9381','J3590') or NDC in ('73650031601','73650031614','73650031610')
order by patid, FST_DT;

select distinct * from ac_dod_TZD_med_claims
order by patid, FST_DT;

-- COMMAND ----------

select count(distinct patid) from ac_dod_TZD_med_claims

-- COMMAND ----------

select distinct ndc, proc_cd, count(distinct patid) from ac_dod_TZD_med_claims
where NDC in ('73650031601','73650031614','73650031610')
group by 1,2
order by 1,2

-- COMMAND ----------

select distinct * from ac_dod_TZD_med_claims
where proc_cd in ('J3590') and ndc in ('73650031614','73650031601')
order by PATID, FST_DT;

-- select distinct * from ac_dod_TZD_med_claims
-- where ndc in ('73650031614')
-- order by PATID, FST_DT;

-- select distinct * from ac_dod_TZD_med_claims
-- where proc_cd in ('J3590')
-- order by PATID, FST_DT;

-- COMMAND ----------

select distinct NDC, max(fst_dt) from ac_dod_TZD_med_claims
where NDC in ('73650031601','73650031614','73650031610')
group by 1
order by 1

-- COMMAND ----------

select distinct * from ac_dod_TZD_med_claims
where PROC_CD='J9381'
order by patid, FST_DT
