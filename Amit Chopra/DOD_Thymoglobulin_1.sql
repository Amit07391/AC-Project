-- Databricks notebook source

-- drop table if exists ac_cdm_diag_202210;
-- CREATE TABLE ac_cdm_diag_202210 USING DELTA LOCATION "/mnt/optumclin/202210/ontology/base/dod/Medical Diagnosis";

drop table if exists ac_dod_2303_med_claims;

create table ac_dod_2303_med_claims using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Medical Claims';

select * from ac_dod_2303_med_claims;

-- drop table if exists ac_cdm_mem_cont_enrol_202210;
-- CREATE TABLE ac_cdm_mem_cont_enrol_202210 USING DELTA LOCATION "/mnt/optumclin/202210/ontology/base/dod/Member Continuous Enrollment";

drop table if exists ac_cdm_RX_claims_202303;
CREATE TABLE ac_cdm_RX_claims_202303 USING DELTA LOCATION "/mnt/optumclin/202303/ontology/base/dod/RX Claims";

select distinct * from ac_cdm_RX_claims_202303;


drop table if exists ac_cdm_NDC_lu_202303;
CREATE TABLE ac_cdm_NDC_lu_202303 USING DELTA LOCATION "dbfs:/mnt/optumclin/202303/ontology/base/dod/Lookup NDC";

select distinct * from ac_cdm_NDC_lu_202303;


-- COMMAND ----------

select distinct * from ac_cdm_NDC_lu_202303
where upper(brnd_nm) like '%THYMOGLOBULIN%' or upper(gnrc_nm) like '%THYMO%' or
upper(brnd_nm) like '%RABBIT%' or upper(gnrc_nm) like '%RABBIT%';

--62053053425
--58468008001

-- select max(fill_dt) from ac_cdm_RX_claims_202303

-- COMMAND ----------

create or replace table ac_dod_thymo_RX_clms as
select distinct * from ac_cdm_RX_claims_202303
where NDC in ('58468008001','62053053425') or upper(brnd_nm) like '%THYMOGLOBULIN%' or upper(gnrc_nm) like '%THYMO%'
order by patid, fill_dt;

select distinct * from ac_dod_thymo_RX_clms
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dod_thymo_med_clms as
select distinct * from ac_dod_2303_med_claims
where proc_cd in ('J7511','J7504') or NDC in ('58468008001','62053053425')
order by patid, fst_dt;


select distinct * from ac_dod_thymo_med_clms
order by patid, fst_dt;
