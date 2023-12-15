-- Databricks notebook source
-- MAGIC %python
-- MAGIC ac_t2d_dashboard_file = "/FileStore/tables/step99_pat_trt_pathway_duration_start_any_demog_sankey_sankey_230406.csv";
-- MAGIC
-- MAGIC ac_t2d_dashboard_1606='dbfs:/FileStore/tables/step99_pat_trt_pathway_duration_start_any_demog_sankey_sol_sankey_soli_230612.csv';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CSV OPTIONS
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "true"
-- MAGIC file_type = "csv"
-- MAGIC delimiter = ","
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ac_t2d_dashboard_file_1 = spark.read.format(file_type).option("inferSchema",infer_schema).option("header",first_row_is_header).option("sep",delimiter).load(ac_t2d_dashboard_file);
-- MAGIC
-- MAGIC ac_t2d_dashboard_file_1606 = spark.read.format(file_type).option("inferSchema",infer_schema).option("header",first_row_is_header).option("sep",delimiter).load(ac_t2d_dashboard_1606)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC ac_t2d_dashboard_file_1.write.mode("overwrite").saveAsTable("ac_t2d_dashboard_file_2");
-- MAGIC ac_t2d_dashboard_file_1606.write.mode("overwrite").saveAsTable("ac_t2d_dashboard_file_1606_file")
-- MAGIC
-- MAGIC

-- COMMAND ----------

select distinct * from ac_t2d_dashboard_file_1606_file

-- COMMAND ----------

-- select bus, count(distinct patid) from ac_t2d_dashboard_file_1606_file
-- group by 1
-- order by 2 desc;

select distinct patid from ac_t2d_dashboard_file_1606_file
where BL_6M_CCI_SCORE < 5 and BL_6M_CCI_SCORE<>'.'
;





-- COMMAND ----------

select distinct BL_6M_DX_ECI_HYPOTHY, count(distinct patid) from ac_t2d_dashboard_file_1606_file
group by 1
order by 1;


select distinct BL_6M_A1C_GRP, count(distinct patid) from ac_t2d_dashboard_file_1606_file
group by 1
order by 1;

-- COMMAND ----------

select distinct * from ac_t2d_dashboard_file_2
order by patid, source

-- COMMAND ----------

select source, source_therapy, source_cat, target_cat, count(distinct patid) as pts from ac_t2d_dashboard_file_2
group by 1,2,3,4
order by 1,2,3,4
;

-- select source_cat,source_therapy,target_cat, target_therapy, count(distinct patid) as pts from ac_t2d_dashboard_file_2
-- group by 1,2,3,4
-- order by 1,2,3,4
-- ;

-- select gender, count(distinct patid), mean(days_s_t) as pts from ac_t2d_dashboard_file_2
-- group by 1
-- order by 1
-- ;

-- COMMAND ----------

select distinct * from ac_t2d_dashboard_file_2
order by patid, source

-- COMMAND ----------

select target_cat,target_therapy, count(distinct patid) from ac_t2d_dashboard_file_2
where target_cat="Line 1"
group by 1,2
order by 3 desc;

select target_cat,time_on_treatment,target_therapy, count(distinct patid) from ac_t2d_dashboard_file_2
where target_cat="Line 1"
group by 1,2,3
order by 4 desc;

-- select source_cat, source_therapy, target_cat,target_therapy, count(distinct patid) from ac_t2d_dashboard_file_2
-- group by 1,2,3,4
-- order by 1,2,3,4;


-- COMMAND ----------

select source_cat, source_therapy, target_therapy, count(distinct patid) from ac_t2d_dashboard_file_2
group by 1,2,3
order by 1,2,3;

select source_cat, count(distinct patid) from ac_t2d_dashboard_file_2
group by 1
order by 1;


-- COMMAND ----------

typ2_diab_icd_cohort = spark.read.format(file_type).option("inferSchema",infer_schema).option("header",first_row_is_header).option("sep",delimiter).load(icd_code_typ2_diab_loc)

typ2_diab_icd_cohort.write.mode("overwrite").saveAsTable("default.type2_diab_icd")

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
