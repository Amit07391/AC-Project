-- Databricks notebook source
drop table if exists ty19_rx_anti_dm; create table ty37_rx_anti_dm as
select distinct a.PATID, a.PAT_PLANID, a.AVGWHLSL, a.CHARGE, a.CLMID, a.COPAY, a.DAW, a.DAYS_SUP, a.DEDUCT, a.DISPFEE, a.FILL_DT, a.MAIL_IND, a.NPI, a.PRC_TYP, a.QUANTITY
        , a.RFL_NBR, a.SPECCLSS, a.STD_COST, a.STD_COST_YR, a.STRENGTH, b.*
from ty37_dod_2208_rx_claim a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
; select * from ty37_rx_anti_dm; select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(fill_dt) as dt_rx_start, max(fill_dt) as dt_rx_stop
from ty37_rx_anti_dm;

-- COMMAND ----------

select distinct * from ty00_ses_rx_anti_dm_loopup

-- COMMAND ----------

----------Import SES Medical Diagnosis records---------

-- drop table if exists ac_dod_2303_med_diag;

-- create table ac_dod_2303_med_diag using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Medical Diagnosis';

-- select * from ac_dod_2303_med_diag;

drop table if exists ac_dod_2303_RX_claim;

create table ac_dod_2303_RX_claim using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/RX Claims';

select * from ac_dod_2303_RX_claim;

-- COMMAND ----------

drop table if exists ac_dod_dx_subset_T2D_full;

create table ac_dod_dx_subset_T2D_full as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_dod_2303_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where b.dx_name='T2DM'
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_subset_T2D_full;

-- COMMAND ----------

select distinct * from sg_antidiabetics_code

-- COMMAND ----------

drop table if exists ac_rx_anti_t2d_dm;
create table ac_rx_anti_t2d_dm as
select distinct a.PATID, a.PAT_PLANID, a.AVGWHLSL, a.CHARGE, a.CLMID, a.COPAY, a.DAW, a.DAYS_SUP, a.DEDUCT, a.DISPFEE, a.FILL_DT, a.MAIL_IND, a.NPI, a.PRC_TYP, a.QUANTITY, 
a.RFL_NBR, a.SPECCLSS, a.STD_COST, a.STD_COST_YR, a.STRENGTH, b.*
from ac_dod_2303_RX_claim a join sg_antidiabetics_code b
on a.ndc=b.ndc_code
order by a.patid, a.fill_dt
;

select * from ac_rx_anti_t2d_dm;

-- COMMAND ----------

-- MAGIC %md #### Checking index date

-- COMMAND ----------

create or replace table ac_dod_dx_subset_T2D_full_indx as
select distinct patid, min(fst_dt) as index_date from ac_dod_dx_subset_T2D_full
where fst_dt between '2019-01-01' and '2021-12-31'
group by 1
order by 1;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md #### T2D patients who took the treatment between 2019-2021

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_dm_2 as
select distinct a.*, b.index_date from ac_rx_anti_t2d_dm a
inner join ac_dod_dx_subset_T2D_full_indx b on a.patid=b.patid
where a.fill_dt between '2019-01-01' and '2021-12-31'
order by a.patid, fill_dt;

select distinct * from ac_dx_rx_anti_t2d_dm_2
order by patid, fill_dt;

-- COMMAND ----------

-- MAGIC %md #### checking naive patients

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_revisit as
select distinct a.*, b.index_date from ac_rx_anti_t2d_dm a
inner join ac_dod_dx_subset_T2D_full_indx b on a.patid=b.patid
where a.fill_dt <b.index_date
order by a.patid, fill_dt;

select distinct * from ac_dx_rx_anti_t2d_revisit
order by patid, fill_dt;

-- COMMAND ----------

-- MAGIC %md #### flagging revisit and naive patients

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_dm_3 as
select distinct a.*,
case when b.patid is not null then 'Revisit'
else 'Naive' end as Naive_revisit_flag
from ac_dx_rx_anti_t2d_dm_2 a
left join ac_dx_rx_anti_t2d_revisit b on a.patid=b.patid
order by a.patid, a.fill_dt;

select distinct * from ac_dx_rx_anti_t2d_dm_3
order by patid, fill_dt;

-- COMMAND ----------

select distinct catgeory from ac_dx_rx_anti_t2d_dm_3

-- COMMAND ----------

-- MAGIC %md #### Finding out the first rx date

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_dm_indx as
select distinct patid, min(fill_dt) as Index_rx_date from ac_dx_rx_anti_t2d_dm_3
group by 1
order by 1;

select distinct * from ac_dx_rx_anti_t2d_dm_indx;

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_dm_4 as
select distinct a.*, b.Index_rx_date
from ac_dx_rx_anti_t2d_dm_3 a
left join ac_dx_rx_anti_t2d_dm_indx b on a.patid=b.patid
order by patid, fill_dt;

select distinct * from ac_dx_rx_anti_t2d_dm_4
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_dm_5 as
select distinct *, date_add(fill_dt, cast(days_sup as int)) as fill_dt_new, lead(fill_dt) OVER (PARTITION BY patid,fill_dt ORDER BY fill_dt) as Lead_fill_dt from ac_dx_rx_anti_t2d_dm_4
order by patid, fill_dt;

select distinct * from ac_dx_rx_anti_t2d_dm_5
order by patid, fill_dt;

-- COMMAND ----------

select distinct * from ac_dx_rx_anti_t2d_dm_6
where patid='33003284205'
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_dm_6 as
select distinct *,
case when lead(fill_dt) over (partition by patid order by fill_dt) < fill_dt_new then fill_dt_new else fill_dt end as Adjusted_fill_dt
from ac_dx_rx_anti_t2d_dm_5
order by patid, fill_dt;

select distinct * from ac_dx_rx_anti_t2d_dm_6
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_dx_rx_anti_t2d_1st_trtmnt as
select distinct a.patid, a.fill_dt, a.catgeory, b.Index_rx_date
from ac_dx_rx_anti_t2d_dm_4 a
inner join ac_dx_rx_anti_t2d_dm_indx b on a.patid=b.patid and a.fill_dt=b.Index_rx_date
order by patid, fill_dt;

select distinct * from ac_dx_rx_anti_t2d_1st_trtmnt
order by patid, fill_dt;

create or replace table ac_dx_rx_anti_t2d_1st_trtmnt_dual as
select distinct patid, count(distinct catgeory) as cts from ac_dx_rx_anti_t2d_1st_trtmnt
group by 1;


create or replace table ac_dx_rx_anti_t2d_1st_2 as
select distinct a.*, case when b.cts>=2 then 'Dual'
else a.catgeory end as Drug_flag
from ac_dx_rx_anti_t2d_1st_trtmnt a
left join ac_dx_rx_anti_t2d_1st_trtmnt_dual b on a.patid=b.patid
order by a.patid;


select distinct * from ac_dx_rx_anti_t2d_1st_2
order by patid, fill_dt;


-- COMMAND ----------

-- create or replace table ac_dx_rx_anti_t2d_1st_trtmnt_2 as
-- select distinct a.*, b.drug_flag from ac_dx_rx_anti_t2d_dm_4 a
-- inner join ac_dx_rx_anti_t2d_1st_2 b on a.patid=b.patid and a.index_rx_date=b.index_rx_date and a.catgeory=b.catgeory
-- order by a.patid, a.fill_dt;

select distinct * from ac_dx_rx_anti_t2d_1st_trtmnt_2
order by patid, fill_dt;

-- COMMAND ----------

select distinct * from ac_dx_rx_anti_t2d_1st_trtmnt_2
where patid='33003284205'
order by patid, fill_dt;

-- COMMAND ----------

datediff(b.index_rx_date, a.index_date) as DAYS_S_T, 
