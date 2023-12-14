-- Databricks notebook source
----------Import SES Medical Diagnosis records---------

-- drop table if exists ac_dod_2301_med_diag;

-- create table ac_dod_2301_med_diag using delta location 'dbfs:/mnt/optumclin/202301/ontology/base/dod/Medical Diagnosis';

-- select * from ac_dod_2301_med_diag;


-- drop table if exists ac_dod_2301_med_claim;

-- create table ac_dod_2301_med_claim using delta location 'dbfs:/mnt/optumclin/202301/ontology/base/dod/Medical Claims';

-- select * from ac_dod_2301_med_claim;


drop table if exists ac_dod_2301_lab_claim;

create table ac_dod_2301_lab_claim using delta location 'dbfs:/mnt/optumclin/202301/ontology/base/dod/Lab Results';

select * from ac_dod_2301_lab_claim;



-- COMMAND ----------

select max(fst_dt) from ac_dod_2301_med_diag

-- COMMAND ----------

drop table if exists ac_dod_dx_subset_00_10;

create table ac_dod_dx_subset_00_10 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_dod_2301_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt<='2010-12-31'
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_subset_00_10;

-- COMMAND ----------

drop table if exists ac_dod_dx_subset_11_16;

create table ac_dod_dx_subset_11_16 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_dod_2301_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt<='2016-12-31' and a.fst_dt>='2011-01-01'
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_subset_11_16;


-- COMMAND ----------

drop table if exists ac_dod_dx_subset_17;

create table ac_dod_dx_subset_17 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_dod_2301_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt<='2017-12-31' and a.fst_dt>='2017-01-01'
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_subset_17;

-- COMMAND ----------

drop table if exists ac_dod_dx_subset_18_22;

create table ac_dod_dx_subset_18_22 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_dod_2301_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt>='2018-01-01'
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_subset_18_22;


-- COMMAND ----------

select gnrc_nm, brnd_nm, count(*) from ty19_ses_2208_ndc_lookup
where lcase(gnrc_nm) like '%insulin pump%'
group by gnrc_nm, brnd_nm
order by gnrc_nm, brnd_nm
;

-- COMMAND ----------

----------Import SES Member Continuous Enrollment records---------
drop table if exists ac_dod_2208_mem_conti;

create table ac_dod_2208_mem_conti using delta location 'dbfs:/mnt/optumclin/202208/ontology/base/dod/Member Continuous Enrollment';

select * from ac_dod_2208_mem_conti;


-- COMMAND ----------

select distinct code, description from ty00_all_dx_comorb
where dx_name in ('T1DM')
;

-- COMMAND ----------

drop table if exists ac_dx_t1dm_index;

create table ac_dx_t1dm_index as
select distinct *, dense_rank() over (partition by patid order by fst_dt) as rank
from (select distinct patid, dx_name, fst_dt from ac_dod_dx_subset_00_10 where dx_name in ('T1DM')
      union
      select distinct patid, dx_name, fst_dt from ac_dod_dx_subset_11_16 where dx_name in ('T1DM')
      union
      select distinct patid, dx_name, fst_dt from ac_dod_dx_subset_17 where dx_name in ('T1DM')
      union
      select distinct patid, dx_name, fst_dt from ac_dod_dx_subset_18_22 where dx_name in ('T1DM')
      )
order by patid, fst_d
;

select count(distinct patid) from ac_dx_t1dm_index;
-- select count(distinct patid) from ty39_dx_t1dm_index;



-- COMMAND ----------

drop table if exists ac_pr_islet;

create table ac_pr_islet as
select distinct patid, fst_dt, proc_cd, dense_rank() over (partition by patid order by fst_dt, proc_cd) as rank
from ac_dod_2301_med_claim
where proc_cd in ('86341','86337')
order by patid, fst_dt
;

select * from ac_pr_islet;

-- COMMAND ----------

drop table if exists ac_pr_dysglycemia;

create table ac_pr_dysglycemia as
select distinct patid, fst_dt, proc_cd, dense_rank() over (partition by patid order by fst_dt, proc_cd) as rank
from ac_dod_2301_med_claim
where proc_cd in ('82951', '82947', '82950', '83036')
order by patid, fst_dt
;

select * from ac_pr_dysglycemia


-- COMMAND ----------

drop table if exists ac_pr_islet_lab;

create table ac_pr_islet_lab as
select distinct patid, fst_dt, TST_DESC, loinc_cd, proc_cd, dense_rank() over (partition by patid order by fst_dt, proc_cd) as rank
from ac_dod_2301_lab_claim
where proc_cd in ('86341','86337')
order by patid, fst_dt
;

select * from ac_pr_islet_lab;



-- COMMAND ----------

drop table if exists ac_pr_dysglycemia_lab;

create table ac_pr_dysglycemia_lab as
select distinct patid, fst_dt,TST_DESC, loinc_cd, proc_cd, dense_rank() over (partition by patid order by fst_dt, proc_cd) as rank
from ac_dod_2301_lab_claim
where proc_cd in ('82951', '82947', '82950', '83036')
order by patid, fst_dt
;

select * from ac_pr_dysglycemia_lab;


-- COMMAND ----------

-- MAGIC %md ####Combining lab and med claim for islet test

-- COMMAND ----------

-- create or replace table ac_pr_islet_lab_med_clm as
-- select distinct patid, fst_dt, proc_cd from ac_pr_islet
-- union
-- select distinct patid, fst_dt, proc_cd from ac_pr_islet_lab;

-- select distinct * from ac_pr_islet_lab_med_clm
-- order by patid, fst_dt;

-- create or replace table ac_pr_islet_lab_med_clm_2 as 
-- select distinct patid, fst_dt, proc_cd, dense_rank() over (partition by patid order by fst_dt, proc_cd) as rank 
-- from ac_pr_islet_lab_med_clm
-- order by 1,2;

select distinct * from ac_pr_islet_lab_med_clm_2
order by patid, fst_dt;

create or replace table ac_pr_islet_lab_med_clm_3 as
select distinct patid, count(distinct fst_dt) as n_islet
from ac_pr_islet_lab_med_clm_2
group by 1;

select distinct * from ac_pr_islet_lab_med_clm_3
where patid='33003290868'
order by patid;

-- COMMAND ----------

-- select distinct patid, min(fst_dt) as dt_1st_pr_islet, count(distinct fst_dt) as n_pr_islet from ty39_pr_islet where rank<=2 group by patid;
-- 33003290868
select distinct * from ty39_islet_dysgly_t1dm;

-- select count(distinct patid) as N from ty39_islet_dysgly_t1dm where isnotnull(dt_1st_pr_islet) and n_pr_islet>=2

create or replace table ac_tsts_islet as
select distinct a.patid, min(a.dt_1st_pr_islet) as dt_1st_pr_islet, max(a.n_pr_islet) as n_pr_islet from (
select distinct patid, min(fst_dt) as dt_1st_pr_islet, count(distinct fst_dt) as n_pr_islet from ac_pr_islet_lab_med_clm_2 where rank<=2 group by patid) a group by 1;

select distinct * from ac_tsts_islet
where patid='33003290868';


select '1. Presence of two or more islet antibodies and normal blood sugar level' as Step, count(distinct patid) as N, min(dt_1st_pr_islet) as dt_start, max(dt_1st_pr_islet) as dt_stop from ac_tsts_islet where isnotnull(dt_1st_pr_islet) and n_pr_islet>=2;

-- COMMAND ----------

select count(distinct a.patid) from ac_pr_islet_lab_med_clm_3 a
inner join ac_dx_t1dm_index b on a.patid=b.patid;
-- where n_islet >=2;

select count(distinct a.patid) from ac_pr_islet_lab_med_clm_3 a
-- inner join ac_dx_t1dm_index b on a.patid=b.patid
where n_islet >=2;

-- COMMAND ----------

-- MAGIC %md ####Combining lab and med claim for dysglycemia test

-- COMMAND ----------

create or replace table ac_pr_dysglycemia_lab_med_clm as
select distinct patid, fst_dt, proc_cd from ac_pr_dysglycemia
union
select distinct patid, fst_dt, proc_cd from ac_pr_dysglycemia_lab;

select distinct * from ac_pr_dysglycemia_lab_med_clm
order by patid, fst_dt;

create or replace table ac_pr_dysglycemia_lab_med_clm_2 as 
select distinct patid, fst_dt, proc_cd, dense_rank() over (partition by patid order by fst_dt, proc_cd) as rank 
from ac_pr_dysglycemia_lab_med_clm
order by 1,2;

select distinct * from ac_pr_dysglycemia_lab_med_clm_2
order by patid, fst_dt;

create or replace table ac_pr_dysglycemia_lab_med_clm_3 as
select distinct patid, count(distinct fst_dt) as n_dysgly
from ac_pr_dysglycemia_lab_med_clm_2
group by 1;


-- COMMAND ----------

select count(distinct a.patid) from ac_pr_dysglycemia_lab_med_clm_3 a
inner join ac_dx_t1dm_index b on a.patid=b.patid;
-- where n_dysgly >=2;

select count(distinct a.patid) as cnts from ac_pr_islet_lab_med_clm_3 a
inner join ac_pr_dysglycemia_lab_med_clm_3 b on a.patid=b.patid
inner join ac_dx_t1dm_index c on a.patid=c.patid
where a.n_islet>=2;

-- COMMAND ----------

drop table if exists ty39_islet_dysgly_t1dm;

create table ty39_islet_dysgly_t1dm as
select distinct a.patid, min(a.dt_1st_pr_islet) as dt_1st_pr_islet, max(a.n_pr_islet) as n_pr_islet, min(b.fst_dt) as dt_1st_pr_dysglycemia, min(c.fst_dt) as dt_1st_dx_t1dm
from (select distinct patid, min(fst_dt) as dt_1st_pr_islet, count(distinct fst_dt) as n_pr_islet from ty39_pr_islet where rank<=2 group by patid) a
left join (select * from ty39_pr_dysglycemia where rank=1) b on a.patid=b.patid
left join (select * from ty39_dx_t1dm_index where rank=1) c on a.patid=c.patid
group by a.patid
order by a.patid
;

select * from ty39_islet_dysgly_t1dm;


-- COMMAND ----------

select distinct n_pr_islet from ty39_islet_dysgly_t1dm

-- COMMAND ----------

drop table if exists ty39_pat_t1dm_attrition;

create table ty39_pat_t1dm_attrition as
select '1. Presence of two or more islet antibodies and normal blood sugar level' as Step, count(distinct patid) as N, min(dt_1st_pr_islet) as dt_start, max(dt_1st_pr_islet) as dt_stop from ty39_islet_dysgly_t1dm where isnotnull(dt_1st_pr_islet) and n_pr_islet>=2
union
select '2. Disease becomes associated with glucose intolerance, or dysglycemia' as Step, count(distinct patid) as N, min(dt_1st_pr_dysglycemia) as dt_start, max(dt_1st_pr_dysglycemia) as dt_stop from ty39_islet_dysgly_t1dm where isnotnull(dt_1st_pr_islet) and n_pr_islet>=2 and dt_1st_pr_islet<=dt_1st_pr_dysglycemia
union
select '3. Time of clinical diagnosis' as Step, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_start, max(dt_1st_dx_t1dm) as dt_stop from ty39_islet_dysgly_t1dm where isnotnull(dt_1st_pr_islet) and n_pr_islet>=2 and dt_1st_pr_islet<=dt_1st_pr_dysglycemia and dt_1st_pr_dysglycemia<=dt_1st_dx_t1dm
order by step
;

select * from ty39_pat_t1dm_attrition;


-- COMMAND ----------

select * from ty37_dod_2208_mem_conti;


-- COMMAND ----------

drop table if exists ty39_islet_dysgly_t1dm_demog;

create table ty39_islet_dysgly_t1dm_demog as
select distinct a.*, b.eligeff, b.eligend, b.gdr_cd, b.race, b.yrdob, datediff(a.dt_1st_pr_dysglycemia,a.dt_1st_pr_islet) as days_islet_2_dysglycemia
, datediff(a.dt_1st_dx_t1dm,a.dt_1st_pr_dysglycemia) as days_dysglycemia_2_t1dm
, datediff(a.dt_1st_dx_t1dm,a.dt_1st_pr_islet) as days_islet_2_t1dm, year(a.dt_1st_dx_t1dm)-b.yrdob as age_on_t1dm
, case when year(a.dt_1st_dx_t1dm)-b.yrdob<18 and isnotnull(year(a.dt_1st_dx_t1dm)-b.yrdob) then 'Age  < 18'
       when year(a.dt_1st_dx_t1dm)-b.yrdob>=18 and year(a.dt_1st_dx_t1dm)-b.yrdob<41 then 'Age 18 - 40'
       when year(a.dt_1st_dx_t1dm)-b.yrdob>=41 and year(a.dt_1st_dx_t1dm)-b.yrdob<65 then 'Age 41 - 64'
       when year(a.dt_1st_dx_t1dm)-b.yrdob>=65  then 'Age 65+'
       else null end as age_grp_t1dm
from (select distinct * from ty39_islet_dysgly_t1dm where isnotnull(dt_1st_pr_islet) and n_pr_islet>=2 and dt_1st_pr_islet<=dt_1st_pr_dysglycemia and dt_1st_pr_dysglycemia<=dt_1st_dx_t1dm) a
left join ty37_dod_2208_mem_conti b on a.patid=b.patid and a.dt_1st_dx_t1dm between b.eligeff and b.eligend
order by a.patid
;

select * from ty39_islet_dysgly_t1dm_demog;

-- COMMAND ----------

drop table if exists ty39_t1dm_duration;

create table ty39_t1dm_duration as
select '1. Duration (days) from 1st Islet to 1st Dysglycemia' as Duration, count(distinct patid) as N, mean(days_islet_2_dysglycemia) as mean_days, std(days_islet_2_dysglycemia) as std_days, min(days_islet_2_dysglycemia) as min_days, percentile(days_islet_2_dysglycemia, 0.25) as p25_days, percentile(days_islet_2_dysglycemia, 0.5) as median_days, percentile(days_islet_2_dysglycemia, 0.75) as p75_days, max(days_islet_2_dysglycemia) as max_days from ty39_islet_dysgly_t1dm_demog
union
select '2. Duration (days) from 1st Dysglycemia to 1st T1DM' as Duration, count(distinct patid) as N, mean(days_dysglycemia_2_t1dm) as mean_days, std(days_dysglycemia_2_t1dm) as std_days, min(days_dysglycemia_2_t1dm) as min_days, percentile(days_dysglycemia_2_t1dm, 0.25) as p25_days, percentile(days_dysglycemia_2_t1dm, 0.5) as median_days, percentile(days_dysglycemia_2_t1dm, 0.75) as p75_days, max(days_dysglycemia_2_t1dm) as max_days from ty39_islet_dysgly_t1dm_demog
union
select '3. Duration (days) from 1st Islet to 1st T1DM' as Duration, count(distinct patid) as N, mean(days_islet_2_t1dm) as mean_days, std(days_islet_2_t1dm) as std_days, min(days_islet_2_t1dm) as min_days, percentile(days_islet_2_t1dm, 0.25) as p25_days, percentile(days_islet_2_t1dm, 0.5) as median_days, percentile(days_islet_2_t1dm, 0.75) as p75_days, max(days_islet_2_t1dm) as max_days from ty39_islet_dysgly_t1dm_demog
order by Duration
;

select * ty39_t1dm_duration;

-- COMMAND ----------

select * from ty39_t1dm_duration;


-- COMMAND ----------

drop table if exists ty39_t1dm_demog_summary;

create table ty39_t1dm_demog_summary as
select '1. Total number of patients' as Cat, count(distinct patid) as N_mean, 100 as pct_std from ty39_islet_dysgly_t1dm_demog
union
select '2.  Age on T1DM' as Cat, mean(age_on_t1dm) as N_mean, std(age_on_t1dm) as pct_std from ty39_islet_dysgly_t1dm_demog
--union
--select age_grp_t1dm as Cat, count(distinct patid) as N_mean, count(distinct patid) as pct_std from ty39_islet_dysgly_t1dm_demog group by age_grp_t1dm order by age_grp_t1dm
--union
--select gdr_cd as Cat, count(distinct patid) as N_mean, count(distinct patid) as pct_std from ty39_islet_dysgly_t1dm_demog group by gdr_cd order by gdr_cd
;

select * from ty39_t1dm_demog_summary;

-- COMMAND ----------

select age_grp_t1dm as Cat, count(distinct patid) as N_mean from ty39_islet_dysgly_t1dm_demog group by age_grp_t1dm order by age_grp_t1dm

-- COMMAND ----------

select gdr_cd as Cat, count(distinct patid) as N_mean from ty39_islet_dysgly_t1dm_demog group by gdr_cd order by gdr_cd

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty39_islet_dysgly_t1dm_demog")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty39_islet_dysgly_t1dm_demog")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty39_t1dm_duration")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty39_t1dm_duration")
-- MAGIC
-- MAGIC display(df)
