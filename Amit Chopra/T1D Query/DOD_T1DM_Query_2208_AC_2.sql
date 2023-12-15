-- Databricks notebook source
----------Import SES Medical Diagnosis records---------

drop table if exists ac_dod_2208_med_diag;

create table ac_dod_2208_med_diag using delta location 'dbfs:/mnt/optumclin/202208/ontology/base/dod/Medical Diagnosis';

select * from ac_dod_2208_med_diag;

-- COMMAND ----------

drop table if exists ty37_dod_dx_subset_00_10;

create table ty37_dod_dx_subset_00_10 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ty37_dod_2208_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt<='2010-12-31'
order by a.patid, a.fst_dt
;

select * from ty37_dod_dx_subset_00_10;

-- COMMAND ----------

drop table if exists ty37_dod_dx_subset_11_16;

create table ty37_dod_dx_subset_11_16 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ty37_dod_2208_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt<='2016-12-31' and a.fst_dt>='2011-01-01'
order by a.patid, a.fst_dt
;

select * from ty37_dod_dx_subset_11_16;


-- COMMAND ----------

drop table if exists ty37_dod_dx_subset_17;

create table ty37_dod_dx_subset_17 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ty37_dod_2208_med_diag a join ty00_all_dx_comorb b
on a.diag=b.code
where a.fst_dt<='2017-12-31' and a.fst_dt>='2017-01-01'
order by a.patid, a.fst_dt
;

select * from ty37_dod_dx_subset_17;

-- COMMAND ----------

drop table if exists ac_dod_dx_subset_18_22;

create table ac_dod_dx_subset_18_22 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                , b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_dod_2208_med_diag a join ty00_all_dx_comorb b
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

select dx_name from ac_dod_dx_subset_18_22
group by dx_name
order by dx_name
;

-- COMMAND ----------

drop table if exists ac_dx_t1dm_hypo;

create table ac_dx_t1dm_hypo as
select distinct *
from ac_dod_dx_subset_18_22
where dx_name in ('HYPO','T1DM') and fst_dt>='2021-01-01' and fst_dt<='2021-12-31'
order by patid, fst_dt
;

select * from ac_dx_t1dm_hypo;


-- COMMAND ----------

select max(fill_dt) from ty37_rx_anti_dm;
select distinct diag from ac_dx_t1dm_hypo
where dx_name ='HYPO';

-- COMMAND ----------

select distinct ndc from ty37_rx_anti_dm
where rx_type in ('Basal','Bolus','PreMix')
;


-- COMMAND ----------

-- MAGIC %md #### Getting the max a1c value

-- COMMAND ----------

-- create or replace table ac_lab_a1c_loinc_max_21_value as
-- select distinct patid, max(value) as Value1 from ty37_lab_a1c_loinc_value
-- where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31'
-- group by patid
-- order by 1;

-- select distinct * from ac_lab_a1c_loinc_max_21_value
-- order by 1;

create or replace table ac_lab_a1c_loinc_21_value_2 as
select distinct a.*,b.value1 from ty37_lab_a1c_loinc_value a
inner join ac_lab_a1c_loinc_max_21_value b on a.patid=b.patid
where '2021-01-01'<=a.fst_dt and a.fst_dt<='2021-12-31'
order by a.patid, a.fst_dt;

select distinct * from ac_lab_a1c_loinc_21_value_2
order by patid, fst_dt;

-- COMMAND ----------

select distinct *
from ac_lab_a1c_loinc_21_value_2
where  value>=7
order by patid, fst_dt;

-- COMMAND ----------

drop table if exists ac_t1dm_etc;

create table ac_t1dm_etc as
select distinct a.patid, a.dt_1st_dx_t1dm, a.n_dx_t1dm, b.dt_1st_dx_hypo, b.n_dx_hypo, c.n_lb_a1c_ge75, d.n_lb_a1c_ge9, e.n_lb_a1c_le7, m.n_lb_a1c_ge7, f.n_lb_a1c, g.n_rx_insulin, h.eligeff, h.eligend, h.gdr_cd, h.race, h.yrdob
, case when isnotnull(e.n_lb_a1c_le7) then e.n_lb_a1c_le7/f.n_lb_a1c
       else null end as pct_a1c_in_range
from (select distinct patid, min(fst_dt) as dt_1st_dx_t1dm, count(distinct fst_dt) as n_dx_t1dm from ac_dx_t1dm_hypo where dx_name='T1DM' group by patid) a
left join (select distinct patid, min(fst_dt) as dt_1st_dx_hypo, count(distinct fst_dt) as n_dx_hypo from ac_dx_t1dm_hypo where dx_name='HYPO' group by patid) b on a.patid=b.patid
left join (select distinct patid, count(distinct fst_dt) as n_lb_a1c_ge75 from ty37_lab_a1c_loinc_value where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value>=7.5 group by patid) c on a.patid=c.patid
left join (select distinct patid, count(distinct fst_dt) as n_lb_a1c_ge9  from ty37_lab_a1c_loinc_value where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value>=9   group by patid) d on a.patid=d.patid
left join (select distinct patid, count(distinct fst_dt) as n_lb_a1c_le7  from ty37_lab_a1c_loinc_value where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value<=7 and isnotnull(value) group by patid) e on a.patid=e.patid
left join (select distinct patid, count(distinct fst_dt) as n_lb_a1c_ge7  from ty37_lab_a1c_loinc_value where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value>=7 and isnotnull(value) group by patid) m on a.patid=m.patid
left join (select distinct patid, count(distinct fst_dt) as n_lb_a1c      from ty37_lab_a1c_loinc_value where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and isnotnull(value) group by patid) f on a.patid=f.patid
-- left join (select distinct patid, count(distinct fill_dt) as n_rx_insulin from ty37_rx_anti_dm where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31' and rx_type in ('Basal','Bolus','PreMix') group by patid) g on a.patid=g.patid
left join (select distinct patid, count(distinct fill_dt) as n_rx_insulin from ac_rx_anti_RAI_dm_1 where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31' group by patid) g on a.patid=g.patid
left join (select distinct patid, eligeff, eligend, gdr_cd, race, yrdob from ac_dod_2208_mem_conti) h on a.patid=h.patid and a.dt_1st_dx_t1dm between h.eligeff and h.eligend
order by a.patid
;

select * from ac_t1dm_etc;

-- COMMAND ----------

drop table if exists ac_t1dm_etc;

create table ac_t1dm_etc as
select distinct a.patid, a.dt_1st_dx_t1dm, a.n_dx_t1dm, b.dt_1st_dx_hypo, b.n_dx_hypo, c.n_lb_a1c_ge75, d.n_lb_a1c_ge9, e.n_lb_a1c_le7, m.n_lb_a1c_ge7, f.n_lb_a1c, g.n_rx_insulin, h.eligeff, h.eligend, h.gdr_cd, h.race, h.yrdob
, case when isnotnull(e.n_lb_a1c_le7) then e.n_lb_a1c_le7/f.n_lb_a1c
       else null end as pct_a1c_in_range
from (select distinct patid, min(fst_dt) as dt_1st_dx_t1dm, count(distinct fst_dt) as n_dx_t1dm from ac_dx_t1dm_hypo where dx_name='T1DM' group by patid) a
left join (select distinct patid, min(fst_dt) as dt_1st_dx_hypo, count(distinct fst_dt) as n_dx_hypo from ac_dx_t1dm_hypo where dx_name='HYPO' group by patid) b on a.patid=b.patid
left join (select distinct patid, 1 as n_lb_a1c_ge75 from ac_lab_a1c_loinc_21_value_2 where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value1>=7.5 group by patid) c on a.patid=c.patid
left join (select distinct patid, 1 as n_lb_a1c_ge9  from ac_lab_a1c_loinc_21_value_2 where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value1>=9   group by patid) d on a.patid=d.patid
left join (select distinct patid, count(distinct fst_dt) as n_lb_a1c_le7  from ac_lab_a1c_loinc_21_value_2 where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value<=7 and isnotnull(value) group by patid) e on a.patid=e.patid
left join (select distinct patid, 1 as n_lb_a1c_ge7  from ac_lab_a1c_loinc_21_value_2 where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and value1>=7 and isnotnull(value1) group by patid) m on a.patid=m.patid
left join (select distinct patid, count(distinct fst_dt) as n_lb_a1c      from ac_lab_a1c_loinc_21_value_2 where '2021-01-01'<=fst_dt and fst_dt<='2021-12-31' and isnotnull(value) group by patid) f on a.patid=f.patid
-- left join (select distinct patid, count(distinct fill_dt) as n_rx_insulin from ty37_rx_anti_dm where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31' and rx_type in ('Basal','Bolus','PreMix') group by patid) g on a.patid=g.patid
left join (select distinct patid, count(distinct fill_dt) as n_rx_insulin from ac_rx_anti_RAI_dm_1 where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31' group by patid) g on a.patid=g.patid
left join (select distinct patid, eligeff, eligend, gdr_cd, race, yrdob from ac_dod_2208_mem_conti) h on a.patid=h.patid and a.dt_1st_dx_t1dm between h.eligeff and h.eligend
order by a.patid
;

select * from ac_t1dm_etc;

-- COMMAND ----------

drop table if exists ac_t1dm_freq;

create table ac_t1dm_freq as
select '1. # of people with Type 1 diabetes' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc
union
select '2. # of people with Type 1 diabetes continued enrolled in year 2021' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '2.1. Have A1C >=7%' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_lb_a1c_ge7) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '3. Have A1C >=7.5%' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_lb_a1c_ge75) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '4. Have A1C >=9.0%' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_lb_a1c_ge9 ) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '5. Have >3 severe hypoglycemia episodes in 1 year' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where n_dx_hypo>3 and isnotnull(n_lb_a1c) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '6. Have A1c' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_lb_a1c)  and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '6a. Have a time in range <=60%' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_lb_a1c_le7) and pct_a1c_in_range<0.6 and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '7. Using a pump' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_rx_insulin) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '8. Using a pump with A1C >=7.5%' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_rx_insulin) and isnotnull(n_lb_a1c_ge75) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '9. Using a pump with A1C >=9.0%' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_rx_insulin) and isnotnull(n_lb_a1c_ge9 ) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
union
select '10. Using a pump with A1C >=7.0%' as Cat, count(distinct patid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_t1dm_etc where isnotnull(n_rx_insulin) and isnotnull(n_lb_a1c_ge7 ) and eligeff<='2021-01-01' and '2021-12-31'<=eligend
order by cat
;

select * from ac_t1dm_freq;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty39_t1dm_freq")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty39_t1dm_freq")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty39_dx_t1dm_index;

create table ty39_dx_t1dm_index as
select distinct *, dense_rank() over (partition by patid order by fst_dt) as rank
from (select distinct patid, dx_name, fst_dt from ty37_dod_dx_subset_00_10 where dx_name in ('T1DM')
      union
      select distinct patid, dx_name, fst_dt from ty37_dod_dx_subset_11_16 where dx_name in ('T1DM')
      union
      select distinct patid, dx_name, fst_dt from ty37_dod_dx_subset_17 where dx_name in ('T1DM')
      union
      select distinct patid, dx_name, fst_dt from ty37_dod_dx_subset_18_22 where dx_name in ('T1DM')
      )
order by patid, fst_dt
;

select * from ty39_dx_t1dm_index;


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

select * from ty39_pat_t1dm_attritionty37_dod_2208_mem_conti;


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
