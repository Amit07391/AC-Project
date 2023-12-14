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

drop table if exists ac_dod_2307_member_cont_enrol;
create table ac_dod_2307_member_cont_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Continuous Enrollment';

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





-- COMMAND ----------

select distinct * from ac_dod_2307_lu_diag
where upper(diag_desc) like '%KNEE%'

-- COMMAND ----------

select distinct * from ac_dod_2307_lu_ndc

-- COMMAND ----------

select distinct * from ac_dod_2307_lu_ndc
where lower(brnd_nm) like '%methylprednisolone acetate%' or lower(GNRC_NM) like '%methylprednisolone acetate%';

select distinct * from ac_dod_2307_lu_proc
where lower(PROC_DESC) like '%methylprednisolone%' or lower(PROC_DESC) like '%methylprednisolone%';

-- COMMAND ----------

select distinct * from ac_dod_2307_lu_ndc
where lower(brnd_nm) like '%triamcinolone%' or lower(GNRC_NM) like '%triamcinolone%';

-- select distinct * from ac_dod_2307_lu_proc
-- where lower(PROC_DESC) like '%triamcinolone%' or lower(PROC_DESC) like '%triamcinolone%';

-- COMMAND ----------

select distinct * from ac_dod_2307_lu_ndc
where lower(brnd_nm) like '%betamethasone%' or lower(GNRC_NM) like '%betamethasone%'

-- COMMAND ----------

select distinct * from ac_dod_2307_lu_proc
where lower(PROC_DESC) like '%betamethasone%' or lower(PROC_DESC) like '%betamethasone%';

-- COMMAND ----------

select distinct * from ac_dod_2307_lu_ndc
where lower(brnd_nm) like '%triamcinolone hexacetonide%' or lower(GNRC_NM) like '%triamcinolone hexacetonide%';

select distinct * from ac_dod_2307_lu_proc
where lower(PROC_DESC) like '%triamcinolone hexacetonide%' or lower(PROC_DESC) like '%triamcinolone hexacetonide%';


-- COMMAND ----------

select distinct * from ac_dod_2307_lu_ndc
where lower(brnd_nm) like '%dexamethasone%' or lower(GNRC_NM) like '%dexamethasone%';

select distinct * from ac_dod_2307_lu_proc
where (lower(PROC_DESC) like '%dexamethasone%' or lower(PROC_DESC) like '%dexamethasone%') and proc_typ_cd='HCPCS';


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

-- COMMAND ----------

-- MAGIC %python
-- MAGIC HCPCS_OA= "dbfs:/FileStore/HCPCS_OA_List.csv";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC OA_HCPCS_List = spark.read.format(file_type).option("inferSchema",infer_schema).option("header",first_row_is_header).option("sep",delimiter).load(HCPCS_OA)
-- MAGIC
-- MAGIC OA_HCPCS_List.write.mode("overwrite").saveAsTable("default.OA_HCPCS_List")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC NDC_OA = "dbfs:/FileStore/NDC_List_OA.csv";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "true"
-- MAGIC file_type = "csv"
-- MAGIC delimiter = ","

-- COMMAND ----------

-- MAGIC %python
-- MAGIC OA_NDC_List = spark.read.format(file_type).option("inferSchema",infer_schema).option("header",first_row_is_header).option("sep",delimiter).load(NDC_OA)
-- MAGIC
-- MAGIC OA_NDC_List.write.mode("overwrite").saveAsTable("default.OA_NDC_List")

-- COMMAND ----------

select distinct * from OA_NDC_List

-- COMMAND ----------



drop table if exists ac_dod_dx_knee_OA;

create table ac_dod_dx_knee_OA as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(fst_dt) as yr_dt
from ac_dod_2307_med_diag a
where   a.diag like 'M17%'
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_knee_OA;

-- COMMAND ----------

create or replace table ac_dod_dx_knee_OA_18_20 as
select distinct * from ac_dod_dx_knee_OA
where fst_dt>='2018-01-01' and fst_dt<='2020-12-31'
order by patid, fst_dt;

select distinct * from ac_dod_dx_knee_OA_18_20
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_dx_knee_OA_18_20; -- 1651498

-- COMMAND ----------

create or replace table ac_dod_dx_knee_OA_18_20_index as
select distinct patid, min(fst_dt) as index_date from ac_dod_dx_knee_OA_18_20
group by patid
order by patid;

select distinct * from ac_dod_dx_knee_OA_18_20_index
order by patid;

-- COMMAND ----------

select count(distinct patid) from ac_dod_dx_knee_OA_18_20_index;

-- COMMAND ----------



create or replace table ac_dod_knee_OA_18_20_eligible as
select distinct a.patid, a.index_date, b.eligeff, b.eligend from ac_dod_dx_knee_OA_18_20_index a
left join ac_dod_2307_member_cont_enrol b on a.patid=b.PATID
where b.ELIGEFF<=a.index_date - 365 and b.ELIGEND>=a.index_date +730
order by 1;

select distinct * from ac_dod_knee_OA_18_20_eligible
order by 1;



-- COMMAND ----------

select count(distinct patid) from ac_dod_knee_OA_18_20_eligible; -- 814802

-- COMMAND ----------



drop table if exists ac_dod_dx_knee_OA_diab;

create table ac_dod_dx_knee_OA_diab as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(fst_dt) as yr_dt, b.index_date, c.dx_name
from ac_dod_2307_med_diag a
inner join ac_dod_knee_OA_18_20_eligible b on a.patid=b.patid
join ty00_all_dx_comorb c
on a.diag=c.code
where    c.dx_name in ('T1DM','T2DM') and a.FST_DT>=b.index_date and a.FST_DT<=b.index_date + 730
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_knee_OA_diab
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_OA_cstds_rx_claims as
select distinct a.*, b.description, b.type, c.index_date from ac_dod_2307_rx_claims a
inner join OA_NDC_List b on a.ndc=b.code
inner join ac_dod_knee_OA_18_20_eligible c on a.patid=c.patid
where b.type="NDC" and a.FILL_DT>=c.index_date and a.FILL_DT<=c.index_date + 730
and ndc not in ('72934402308',
'72934435008',
'72934434308',
'00085094205',
'53002061497',
'71300656403',
'00785660531',
'00054817704',
'00054816804',
'00054816816',
'00536045290',
'53002061491',
'00054317757',
'00054817716',
'00054317763',
'71300656401')
order by patid, FILL_DT;

select distinct * from ac_dod_OA_cstds_rx_claims
order by patid, FILL_DT;

-- COMMAND ----------

select max(fill_dt) from ac_dod_OA_cstds_rx_claims

-- COMMAND ----------

create or replace table ac_dod_OA_cstds_med_claims as
select distinct a.*, b.description, b.type, c.index_date from ac_dod_2307_med_claims a
inner join OA_HCPCS_List b on a.PROC_CD=b.code
inner join ac_dod_knee_OA_18_20_eligible c on a.patid=c.patid
where a.FST_DT>=c.index_date and a.fst_dt<=c.index_date + 730
order by a.patid, a.FST_DT;

select distinct * from ac_dod_OA_cstds_med_claims
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from OA_HCPCS_List

-- COMMAND ----------

create or replace table ac_dod_OA_cstds_med_RX_claims as
select distinct patid, clmid, days_sup, fill_dt,ndc, description, type, index_date    from ac_dod_OA_cstds_rx_claims
union
select distinct patid, clmid, 0 as days_sup, fst_dt,proc_cd, description, type, index_date from ac_dod_OA_cstds_med_claims;

select distinct * from ac_dod_OA_cstds_med_RX_claims
order by patid, fill_dt;


-- COMMAND ----------

select distinct patid, count(distinct type) from ac_dod_OA_cstds_med_RX_claims
group by 1
order by 2 desc;

-- COMMAND ----------

select distinct * from ac_dod_OA_cstds_med_RX_claims
where patid='33169074057'
order by fill_dt;

-- COMMAND ----------



select distinct * from ac_dod_OA_cstds_med_claims
where patid='33138357862'
order by FST_DT;

-- COMMAND ----------


create or replace table ac_dod_OA_cstds_freq as
select distinct patid, count(distinct fill_dt) as cnts from ac_dod_OA_cstds_med_RX_claims
group by 1
order by 1;

select distinct * from ac_dod_OA_cstds_freq
order by patid;

-- COMMAND ----------

select distinct * from ac_dod_OA_cstds_med_RX_claims
where patid='33138357862'
order by fill_dt;



-- COMMAND ----------

select
count(distinct patid) from ac_dod_OA_cstds_med_RX_claims


-- COMMAND ----------

select cnts, count(distinct patid) from ac_dod_OA_cstds_freq
group by cnts
order by cnts;

-- COMMAND ----------



drop table if exists ac_dod_dx_knee_OA_diab;

create table ac_dod_dx_knee_OA_diab as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(fst_dt) as yr_dt, b.index_date, c.dx_name
from ac_dod_2307_med_diag a
inner join ac_dod_OA_cstds_med_RX_claims b on a.patid=b.patid
join ty00_all_dx_comorb c
on a.diag=c.code
where    c.dx_name in ('T1DM','T2DM') and a.FST_DT>=b.index_date and a.FST_DT<=b.index_date + 730
order by a.patid, a.fst_dt
;




-- COMMAND ----------

select distinct * from ac_dod_dx_knee_OA_diab
where patid='33028588418'
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_dx_knee_OA_diab_T2D as
select distinct patid, count(distinct fst_dt) as n_t2dm from ac_dod_dx_knee_OA_diab
where dx_name="T2DM"
group by patid
order by patid;

select distinct * from ac_dod_dx_knee_OA_diab_T2D
order by patid;

-- COMMAND ----------

select count(distinct patid) from ac_dod_dx_knee_OA_diab_T2D

-- COMMAND ----------

create or replace table ac_dod_dx_knee_OA_diab_T1D as
select distinct patid, count(distinct fst_dt) as n_t1dm from ac_dod_dx_knee_OA_diab
where dx_name="T1DM"
group by patid
order by patid;

select distinct * from ac_dod_dx_knee_OA_diab_T1D
order by patid;

-- COMMAND ----------

create or replace table ac_dod_dx_knee_OA_T1D_T2D_cnt as
select distinct a.patid, a.n_t2dm, b.n_t1dm from ac_dod_dx_knee_OA_diab_T2D a
inner join ac_dod_dx_knee_OA_diab_T1D b on a.patid=b.patid
where a.n_t2dm>2*b.n_t1dm
order by patid;

select distinct * from ac_dod_dx_knee_OA_T1D_T2D_cnt;

-- COMMAND ----------

select count(distinct patid) as pts from ac_dod_dx_knee_OA_diab_T1D
where patid not in (select distinct patid from ac_dod_dx_knee_OA_T1D_T2D_cnt );
