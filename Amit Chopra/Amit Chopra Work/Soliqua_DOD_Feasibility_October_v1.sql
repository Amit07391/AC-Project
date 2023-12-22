-- Databricks notebook source


-- drop table if exists ac_dod_2309_med_diag;
-- create table ac_dod_2309_med_diag using delta location 'dbfs:/mnt/optumclin/202309/ontology/base/dod/Medical Diagnosis';

-- select distinct * from ac_dod_2309_med_diag;


drop table if exists ac_dod_2308_lu_ndc;
create table ac_dod_2308_lu_ndc using delta location 'dbfs:/mnt/optumclin/202308/ontology/base/dod/Lookup NDC';

select distinct * from ac_dod_2308_lu_ndc;

-- drop table if exists ac_dod_2308_member_enrol;
-- create table ac_dod_2308_member_enrol using delta location 'dbfs:/mnt/optumclin/202308/ontology/base/dod/Member Enrollment';

-- select distinct * from ac_dod_2308_member_enrol;

-- drop table if exists ac_dod_2308_member_cont_enrol;
-- create table ac_dod_2308_member_cont_enrol using delta location 'dbfs:/mnt/optumclin/202308/ontology/base/dod/Member Continuous Enrollment';

-- select distinct * from ac_dod_2308_member_cont_enrol;

-- drop table if exists ac_dod_2308_rx_claims;
-- create table ac_dod_2308_rx_claims using delta location 'dbfs:/mnt/optumclin/202308/ontology/base/dod/RX Claims';

-- select distinct * from ac_dod_2308_rx_claims;

-- drop table if exists ac_dod_2309_med_claims;
-- create table ac_dod_2309_med_claims using delta location 'dbfs:/mnt/optumclin/202309/ontology/base/dod/Medical Claims';

-- select distinct * from ac_dod_2309_med_claims;

-- drop table if exists ac_dod_2307_lu_proc;
-- create table ac_dod_2307_lu_proc using delta location 'dbfs:/mnt/optumclin/202309/ontology/base/dod/Lookup Procedure';

-- select distinct * from ac_dod_2307_lu_proc;

-- drop table if exists ac_dod_2307_lu_diag;
-- create table ac_dod_2307_lu_diag using delta location 'dbfs:/mnt/optumclin/202309/ontology/base/dod/Lookup Diagnosis';

-- select distinct * from ac_dod_2307_lu_diag;


-- drop table if exists ac_dod_2307_labs;

-- CREATE TABLE ac_dod_2307_labs USING DELTA LOCATION 'dbfs:/mnt/optumclin/202309/ontology/base/dod/Lab Results';

-- select * from ac_dod_2307_labs;




-- COMMAND ----------

create or replace table ac_dod_2308_rx_anti_dm as
select distinct a.PATID, a.PAT_PLANID, a.brnd_nm, a.AVGWHLSL, a.CHARGE, a.CLMID, a.COPAY, a.DAW, a.DAYS_SUP, a.DEDUCT, a.DISPFEE, a.FILL_DT, a.MAIL_IND, a.NPI, a.PRC_TYP, a.QUANTITY, a.RFL_NBR, a.SPECCLSS, a.STD_COST, a.STD_COST_YR, a.STRENGTH, b.*
from ac_dod_2308_rx_claims a join sg_antidiabetics_codes b

on a.ndc=b.NDC_Code
order by a.patid, a.fill_dt;

select distinct * from ac_dod_2308_rx_anti_dm
order by patid, fill_dt;

-- COMMAND ----------

select distinct * from ty00_ses_rx_anti_dm_loopup
where
lower(brnd_nm) like ('%soliqua%')

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_17_23 as
select distinct * from ac_dod_2308_rx_anti_dm
where lower(brnd_nm) like ('%soliqua%') and FILL_DT >='2017-01-01'
order by patid, FILL_DT;

select distinct * from ac_dod_soliqua_rx_clm_17_23
order by patid, FILL_DT;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_17_23_indx as
select distinct patid, min(fill_dt) as indx_dt from ac_dod_soliqua_rx_clm_17_23
group by 1
order by 1;

select distinct * from ac_dod_soliqua_rx_clm_17_23_indx
order by 1;

-- COMMAND ----------

select count(distinct patid) from ac_dod_soliqua_rx_clm_17_23_indx

-- COMMAND ----------

select max(fill_dt) from ac_dod_soliqua_rx_clm_17_23

-- COMMAND ----------

create or replace table ac_dod_soliqua_17_23_elig_6m_bl as
select distinct a.patid, a.indx_dt, b.eligeff, b.eligend from ac_dod_soliqua_rx_clm_17_23_indx a
left join ac_dod_2308_member_cont_enrol b on a.patid=b.patid
where b.ELIGEFF<=a.indx_dt - 180 and b.ELIGEND>=a.indx_dt
order by a.patid;

select distinct * from ac_dod_soliqua_17_23_elig_6m_bl
order by patid;

-- COMMAND ----------

select count(distinct patid) from ac_dod_soliqua_17_23_elig_6m_bl

-- COMMAND ----------

create or replace table ac_dod_soliqua_17_23_elig_12m_bl as
select distinct a.patid, a.indx_dt, b.eligeff, b.eligend from ac_dod_soliqua_rx_clm_17_23_indx a
left join ac_dod_2308_member_cont_enrol b on a.patid=b.patid
where b.ELIGEFF<=a.indx_dt - 365 and b.ELIGEND>=a.indx_dt
order by a.patid;

select distinct * from ac_dod_soliqua_17_23_elig_12m_bl
order by patid;

-- COMMAND ----------

select count(distinct patid) from ac_dod_soliqua_17_23_elig_12m_bl

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_6m_bl as 
select distinct a.*, b.indx_dt from ac_dod_2308_rx_anti_dm a
inner join ac_dod_soliqua_17_23_elig_6m_bl b on a.PATID=b.patid
where a.FILL_DT between b.indx_dt - 180 and b.indx_dt - 1
order by a.patid, a.FILL_DT;

select distinct * from ac_dod_soliqua_rx_clm_6m_bl
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_12m_bl as 
select distinct a.*, b.indx_dt from ac_dod_2308_rx_anti_dm a
inner join ac_dod_soliqua_17_23_elig_12m_bl b on a.PATID=b.patid
where a.FILL_DT between b.indx_dt - 365 and b.indx_dt - 1
order by a.patid, a.FILL_DT;

select distinct * from ac_dod_soliqua_rx_clm_12m_bl
order by patid, fill_dt;

-- COMMAND ----------

select distinct * from ac_dod_soliqua_rx_clm_6m_bl where
lower(brnd_nm) like ('%soliqua%')

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_6m_bl_no_OAD as
select distinct * from ac_dod_soliqua_rx_clm_6m_bl
where CATEGORY <> 'OAD'
order by patid, fill_dt;

select distinct * from ac_dod_soliqua_rx_clm_6m_bl_no_OAD
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_6m_bl_OAD as
select distinct * from ac_dod_soliqua_rx_clm_6m_bl
where patid not in (select distinct patid from ac_dod_soliqua_rx_clm_6m_bl_no_OAD)
order by patid, fill_dt;

select distinct * from ac_dod_soliqua_rx_clm_6m_bl_OAD
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_6m_bl_OAD_cnt as
select distinct patid, count(distinct generic_name) as cnt from ac_dod_soliqua_rx_clm_6m_bl_OAD
group by 1
order by patid;

select distinct * from ac_dod_soliqua_rx_clm_6m_bl_OAD_cnt
order by patid;

-- COMMAND ----------

select count(distinct patid) from ac_dod_soliqua_rx_clm_6m_bl_OAD_cnt
where cnt>=3;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_12m_bl_no_OAD as
select distinct * from ac_dod_soliqua_rx_clm_12m_bl
where CATEGORY <> 'OAD'
order by patid, fill_dt;

select distinct * from ac_dod_soliqua_rx_clm_12m_bl_no_OAD
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_12m_bl_OAD as
select distinct * from ac_dod_soliqua_rx_clm_12m_bl
where patid not in (select distinct patid from ac_dod_soliqua_rx_clm_12m_bl_no_OAD)
order by patid, fill_dt;

select distinct * from ac_dod_soliqua_rx_clm_12m_bl_OAD
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_12m_bl_OAD_cnt as
select distinct patid, count(distinct generic_name) as cnt from ac_dod_soliqua_rx_clm_12m_bl_OAD
group by 1
order by patid;

select distinct * from ac_dod_soliqua_rx_clm_12m_bl_OAD_cnt
order by patid;

-- COMMAND ----------

select count(distinct patid) from ac_dod_soliqua_rx_clm_12m_bl_OAD_cnt
where cnt>=3;

-- COMMAND ----------

create or replace table ac_dod_soliqua_rx_clm_20_23 as
select distinct * from ac_dod_2308_rx_anti_dm
where lower(brnd_nm) like ('%soliqua%') and FILL_DT >='2020-06-01' and FILL_DT <='2023-06-30' 
order by patid, FILL_DT;

select distinct * from ac_dod_soliqua_rx_clm_20_23
order by patid, FILL_DT;

-- COMMAND ----------

select count(distinct patid) from ac_dod_soliqua_rx_clm_20_23

-- COMMAND ----------

create or replace table ac_dod_Xultophy_rx_clm_20_23 as
select distinct * from ac_dod_2308_rx_claims
where lower(brnd_nm) like ('%xultophy%') and FILL_DT >='2020-06-01' and FILL_DT <='2023-06-30' 
order by patid, FILL_DT;

select distinct * from ac_dod_Xultophy_rx_clm_20_23
order by patid, FILL_DT;

-- COMMAND ----------

select count(distinct patid) from ac_dod_Xultophy_rx_clm_20_23

-- COMMAND ----------

select distinct * from ty00_ses_rx_anti_dm_loopup
where ndc in ( select distinct NDC_Code from  sg_antidiabetics_codes)

-- COMMAND ----------

select distinct * from ty00_ses_rx_anti_dm_loopup
where ndc in ( select distinct NDC_Code from  sg_antidiabetics_codes)

-- COMMAND ----------

select distinct a.*, b.BRND_NM, b.GNRC_NM from sg_antidiabetics_codes a
left join ac_dod_2308_lu_ndc b on a.NDC_Code=b.ndc

-- COMMAND ----------

select distinct * from ty00_ses_rx_anti_dm_loopup
where ndc not in (select distinct NDC_Code from  sg_antidiabetics_codes) 

-- COMMAND ----------

select distinct * from ty00_ses_rx_anti_dm_loopup

-- COMMAND ----------

select distinct * from ty00_ses_rx_anti_dm_loopup where lower(brnd_nm) like ('%xultophy%')
