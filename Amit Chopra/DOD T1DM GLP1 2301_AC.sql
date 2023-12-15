-- Databricks notebook source
drop table if exists ac_dod_2301_mem_cont_enrol;
create table ac_dod_2301_mem_cont_enrol using delta location 'dbfs:/mnt/optumclin/202301/ontology/base/dod/Member Continuous Enrollment';

-- COMMAND ----------

drop table if exists ac_dx_t2dm;

create table ac_dx_t2dm as
select distinct *
from ac_dod_2301_med_diag
where diag like 'E11%' or (diag like '250%' and substr(diag,5,0) in ('1','2'))
;

select * from ac_dx_t2dm;



-- COMMAND ----------

create or replace table ty41_dx_t1dm_exld_t2d as
select distinct * from ty41_dx_t1dm
where patid not in (select distinct patid from ac_t1d_t2d_test_2);

select count(distinct patid) from ty41_dx_t1dm_exld_t2d
where fst_dt between '2021-01-01' and '2021-12-31';

-- COMMAND ----------

select rx_type, count(*)
from ty41_rx_anti_dm
group by rx_type
order by rx_type
;

-- COMMAND ----------

select *
from ty41_rx_anti_dm
where lcase(brnd_nm) like '%mounjaro%'
;

-- COMMAND ----------

drop table if exists ac_t1dm_trt;

create table ac_t1dm_trt as
select distinct a.patid, a.dt_1st_dx_t1dm, b.dt_1st_rx_basal, b.n_basal, c.dt_1st_rx_bolus, c.n_bolus, d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro, h.eligeff, h.eligend
from (select patid, min(fst_dt) as dt_1st_dx_t1dm from ty41_dx_t1dm_exld_t2d where fst_dt between '2021-01-01' and '2021-12-31' group by patid) a
left join (select patid, min(fill_dt) as dt_1st_rx_basal, count(distinct fill_dt) as n_basal from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Basal' group by patid) b on a.patid=b.patid
left join (select patid, min(fill_dt) as dt_1st_rx_bolus, count(distinct fill_dt) as n_bolus from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Bolus' group by patid) c on a.patid=c.patid
left join (select patid, min(fill_dt) as dt_1st_rx_glp1, count(distinct fill_dt) as n_glp1  from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='GLP1' and lcase(brnd_nm) not like '%mounjaro%' group by patid) d on a.patid=d.patid
left join (select patid, min(fill_dt) as dt_1st_rx_mounjaro, count(distinct fill_dt) as n_mounjaro from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and lcase(brnd_nm) like '%mounjaro%' group by patid) e on a.patid=e.patid
left join (select distinct patid, eligeff, eligend, gdr_cd, race, yrdob from ac_dod_2301_mem_cont_enrol) h on a.patid=h.patid and a.dt_1st_dx_t1dm between h.eligeff and h.eligend
order by a.patid;

select * from ac_t1dm_trt;

-- COMMAND ----------

drop table if exists ac_t2dm_trt;

create table ty44_t2dm_trt as
select distinct a.patid, a.dt_1st_dx_t2dm, b.dt_1st_rx_basal, b.n_basal, c.dt_1st_rx_bolus, c.n_bolus, d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro
from (select patid, min(fst_dt) as dt_1st_dx_t2dm from ty44_dx_t2dm where fst_dt between '2021-01-01' and '2021-12-31' group by patid) a
left join (select patid, min(fill_dt) as dt_1st_rx_basal, count(distinct fill_dt) as n_basal from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Basal' group by patid) b on a.patid=b.patid
left join (select patid, min(fill_dt) as dt_1st_rx_bolus, count(distinct fill_dt) as n_bolus from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Bolus' group by patid) c on a.patid=c.patid
left join (select patid, min(fill_dt) as dt_1st_rx_glp1, count(distinct fill_dt) as n_glp1  from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='GLP1' and lcase(brnd_nm) not like '%mounjaro%' group by patid) d on a.patid=d.patid
left join (select patid, min(fill_dt) as dt_1st_rx_mounjaro, count(distinct fill_dt) as n_mounjaro from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and lcase(brnd_nm) like '%mounjaro%' group by patid) e on a.patid=e.patid
order by a.patid;

select * from ty44_t2dm_trt;


-- COMMAND ----------

drop table if exists ac_t1dm_summary;

create table ac_t1dm_summary as
select count(distinct a.patid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.patid) as n_nasal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_nasal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
, 100*count(distinct c.patid)/count(distinct a.patid) as pct_glp1_mounjaro_t1dm
from (select patid, dt_1st_dx_t1dm, eligeff, eligend from ac_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select patid from ac_t1dm_trt where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) b on a.patid=b.patid
left join (select patid from ac_t1dm_trt where isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) c on a.patid=c.patid
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t1dm_summary;


-- COMMAND ----------

drop table if exists ac_t1dm_obese_summary;

create table ac_t1dm_obese_summary as
select count(distinct a.patid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.patid) as n_nasal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_nasal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
, 100*count(distinct c.patid)/count(distinct a.patid) as pct_glp1_mounjaro_t1dm
from (select patid, dt_1st_dx_t1dm, eligeff, eligend from ac_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select patid from ac_t1dm_trt where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) b on a.patid=b.patid
left join (select patid from ac_t1dm_trt where isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) c on a.patid=c.patid
inner join ac_dod_obese_diag_20_22 d on a.patid=d.PATID
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t1dm_obese_summary;


-- COMMAND ----------

drop table if exists ty44_t1dm_summary;

create table ty44_t1dm_summary as
select count(distinct a.patid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.patid) as n_basal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
, 100*count(distinct c.patid)/count(distinct a.patid) as pct_glp1_mounjaro_t1dm
from (select patid, dt_1st_dx_t1dm from ty44_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select patid from ty44_t1dm_trt where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus)) b on a.patid=b.patid
left join (select patid from ty44_t1dm_trt where isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) c on a.patid=c.patid
;

select * from ty44_t1dm_summary;

-- COMMAND ----------

drop table if exists ac_t1dm_trt;

create table ac_t1dm_trt as
select distinct a.patid, a.dt_1st_dx_t1dm, b.dt_1st_rx_basal, b.n_basal, c.dt_1st_rx_bolus, c.n_bolus, d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro, h.eligeff, h.eligend
from (select patid, min(fst_dt) as dt_1st_dx_t1dm from ty41_dx_t1dm where fst_dt between '2021-01-01' and '2021-12-31' group by patid) a
left join (select patid, min(fill_dt) as dt_1st_rx_basal, count(distinct fill_dt) as n_basal from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Basal' group by patid) b on a.patid=b.patid
left join (select patid, min(fill_dt) as dt_1st_rx_bolus, count(distinct fill_dt) as n_bolus from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Bolus' group by patid) c on a.patid=c.patid
left join (select patid, min(fill_dt) as dt_1st_rx_glp1, count(distinct fill_dt) as n_glp1  from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='GLP1' and lcase(brnd_nm) not like '%mounjaro%' group by patid) d on a.patid=d.patid
left join (select patid, min(fill_dt) as dt_1st_rx_mounjaro, count(distinct fill_dt) as n_mounjaro from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and lcase(brnd_nm) like '%mounjaro%' group by patid) e on a.patid=e.patid
left join (select distinct patid, eligeff, eligend, gdr_cd, race, yrdob from ac_dod_2301_mem_cont_enrol) h on a.patid=h.patid and a.dt_1st_dx_t1dm between h.eligeff and h.eligend
order by a.patid;

select * from ac_t1dm_trt;


-- COMMAND ----------

drop table if exists ac_t2dm_trt_bas_bol;

create table ac_t2dm_trt_bas_bol as
select distinct a.patid, a.dt_1st_dx_t2dm, b.dt_1st_rx_basal, b.n_basal, c.dt_1st_rx_bolus, c.n_bolus, h.eligeff,h.eligend
-- d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro
from (select patid, min(fst_dt) as dt_1st_dx_t2dm from ty44_dx_t2dm where fst_dt between '2021-01-01' and '2021-12-31' group by patid) a
left join (select patid, min(fill_dt) as dt_1st_rx_basal, count(distinct fill_dt) as n_basal from ac_rx_anti_dm_bas_bol_1 where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Basal' group by patid) b on a.patid=b.patid
left join (select patid, min(fill_dt) as dt_1st_rx_bolus, count(distinct fill_dt) as n_bolus from ac_rx_anti_dm_bas_bol_1 where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='Bolus' group by patid) c on a.patid=c.patid
-- left join (select patid, min(fill_dt) as dt_1st_rx_glp1, count(distinct fill_dt) as n_glp1  from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and rx_type='GLP1' and lcase(brnd_nm) not like '%mounjaro%' group by patid) d on a.patid=d.patid
-- left join (select patid, min(fill_dt) as dt_1st_rx_mounjaro, count(distinct fill_dt) as n_mounjaro from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and lcase(brnd_nm) like '%mounjaro%' group by patid) e on a.patid=e.patid
left join (select distinct patid, eligeff, eligend, gdr_cd, race, yrdob from ac_dod_2301_mem_cont_enrol) h on a.patid=h.patid and a.dt_1st_dx_t2dm between h.eligeff and h.eligend
order by a.patid;

select * from ac_t2dm_trt_bas_bol
where patid='33010960959';

-- COMMAND ----------

drop table if exists ac_t2dm_trt_bas_bol_GLP;

create table ac_t2dm_trt_bas_bol_GLP as
select distinct a.patid, a.dt_1st_dx_t2dm,b.drug_flg, h.eligeff,h.eligend
-- d.dt_1st_rx_glp1, d.n_glp1, e.dt_1st_rx_mounjaro, e.n_mounjaro
from (select patid, min(fst_dt) as dt_1st_dx_t2dm from ty44_dx_t2dm where fst_dt between '2021-01-01' and '2021-12-31' group by patid) a
left join (select patid, 1 as drug_flg from ac_rx_anti_dm_bas_bol_GLP_GIP_2 where fill_dt between '2021-01-01' and '2021-12-31'  group by patid) b on a.patid=b.patid
left join (select distinct patid, eligeff, eligend, gdr_cd, race, yrdob from ac_dod_2301_mem_cont_enrol) h on a.patid=h.patid and a.dt_1st_dx_t2dm between h.eligeff and h.eligend
order by a.patid;

select * from ac_t2dm_trt_bas_bol_GLP;

-- COMMAND ----------

drop table if exists ac_t1dm_summary_least2;

create table ac_t1dm_summary_least2 as
select count(distinct a.patid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.patid) as n_nasal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_nasal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
, 100*count(distinct c.patid)/count(distinct a.patid) as pct_glp1_mounjaro_t1dm
from (select patid, dt_1st_dx_t1dm, eligeff, eligend from ac_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select patid from ac_t1dm_trt where (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) and (n_bolus>1 or n_basal>1 or (n_bolus=1 and n_basal=1 ))) b on a.patid=b.patid
left join (select patid from ac_t1dm_trt where (isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (n_glp1>1 or n_mounjaro>1 or (n_glp1=1 and n_mounjaro=1))) c on a.patid=c.patid
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t1dm_summary_least2;


-- COMMAND ----------

drop table if exists ac_t1dm_obese_summary_least2;

create table ac_t1dm_obese_summary_least2 as
select count(distinct a.patid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.patid) as n_nasal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_nasal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
, 100*count(distinct c.patid)/count(distinct a.patid) as pct_glp1_mounjaro_t1dm
from (select patid, dt_1st_dx_t1dm, eligeff, eligend from ac_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select patid from ac_t1dm_trt where (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) and (n_bolus>1 or n_basal>1 or (n_bolus=1 and n_basal=1 ))) b on a.patid=b.patid
left join (select patid from ac_t1dm_trt where (isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (n_glp1>1 or n_mounjaro>1 or (n_glp1=1 and n_mounjaro=1))) c on a.patid=c.patid
inner join ac_dod_obese_diag_20_22 d on a.patid=d.PATID
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t1dm_obese_summary_least2;


-- COMMAND ----------

drop table if exists ty44_t1dm_summary_least2;

create table ty44_t1dm_summary_least2 as
select count(distinct a.patid) as n_t1dm, min(a.dt_1st_dx_t1dm) as dt_t1dm_start, max(a.dt_1st_dx_t1dm) as dt_t1dm_stop, count(distinct b.patid) as n_basal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
, 100*count(distinct c.patid)/count(distinct a.patid) as pct_glp1_mounjaro_t1dm
from (select patid, dt_1st_dx_t1dm from ty44_t1dm_trt where isnotnull(dt_1st_dx_t1dm)) a
left join (select patid from ty44_t1dm_trt where (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) and (n_bolus>1 or n_bolus>1 or (n_bolus=1 and n_basal=1 ))) b on a.patid=b.patid
left join (select patid from ty44_t1dm_trt where (isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (n_glp1>1 or n_mounjaro>1 or (n_glp1=1 and n_mounjaro=1))) c on a.patid=c.patid
;

select * from ty44_t1dm_summary_least2;

-- COMMAND ----------

drop table if exists ac_t2dm_summary_bas_bol;

create table ac_t2dm_summary_bas_bol as
select count(distinct a.patid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.patid) as n_basal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus
from (select patid, dt_1st_dx_t2dm, eligeff,eligend from ac_t2dm_trt_bas_bol where isnotnull(dt_1st_dx_t2dm)) a
left join (select patid, sum(n_basal)+sum(n_bolus) as n_fill_basal_bolus from ac_t2dm_trt_bas_bol where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus) group by patid) b on a.patid=b.patid
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t2dm_summary_bas_bol;

-- COMMAND ----------

drop table if exists ac_t2dm_obese_summary_bas_bol;

create table ac_t2dm_obese_summary_bas_bol as
select count(distinct a.patid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.patid) as n_basal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus
from (select patid, dt_1st_dx_t2dm, eligeff,eligend from ac_t2dm_trt_bas_bol where isnotnull(dt_1st_dx_t2dm)) a
left join (select patid, sum(n_basal)+sum(n_bolus) as n_fill_basal_bolus from ac_t2dm_trt_bas_bol where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus) group by patid) b on a.patid=b.patid
inner join ac_dod_obese_diag_20_22 d on a.patid=d.PATID
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t2dm_obese_summary_bas_bol;

-- COMMAND ----------

drop table if exists ac_t2dm_summary_bas_bol_GLP;

create table ac_t2dm_summary_bas_bol_GLP as
select count(distinct a.patid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.patid) as n_basal_bolus, count(distinct c.patid) as n_basal_bolus_GLP
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus,
100*count(distinct c.patid)/count(distinct b.patid) as pct_basal_bolus_GLP
from (select patid, dt_1st_dx_t2dm, eligeff,eligend from ac_t2dm_trt_bas_bol_GLP where isnotnull(dt_1st_dx_t2dm)) a
left join (select patid, sum(n_basal)+sum(n_bolus) as n_fill_basal_bolus from ac_t2dm_trt_bas_bol where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus) group by patid) b on a.patid=b.patid
left join (select patid from ac_t2dm_trt_bas_bol_GLP where isnotnull(drug_flg)  group by patid) c on a.patid=c.patid
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t2dm_summary_bas_bol_GLP;

-- COMMAND ----------

drop table if exists ac_t2dm_obese_summary_bas_bol_GLP;

create table ac_t2dm_obese_summary_bas_bol_GLP as
select count(distinct a.patid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.patid) as n_basal_bolus, count(distinct c.patid) as n_basal_bolus_GLP
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus,
100*count(distinct c.patid)/count(distinct b.patid) as pct_basal_bolus_GLP
from (select patid, dt_1st_dx_t2dm, eligeff,eligend from ac_t2dm_trt_bas_bol_GLP where isnotnull(dt_1st_dx_t2dm)) a
left join (select patid, sum(n_basal)+sum(n_bolus) as n_fill_basal_bolus from ac_t2dm_trt_bas_bol where isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus) group by patid) b on a.patid=b.patid
left join (select patid from ac_t2dm_trt_bas_bol_GLP where isnotnull(drug_flg)  group by patid) c on a.patid=c.patid
inner join ac_dod_obese_diag_20_22 d on a.patid=d.PATID
where a.eligeff<='2021-01-01' and '2021-12-31'<=a.eligend
;

select * from ac_t2dm_obese_summary_bas_bol_GLP;

-- COMMAND ----------

select * from ty44_t2dm_trt;

-- COMMAND ----------

drop table if exists ac_t2dm_summary_least2;

create table ac_t2dm_summary_least2 as
select count(distinct a.patid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.patid) as n_nasal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_nasal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
from (select patid, dt_1st_dx_t2dm from ty44_t2dm_trt where isnotnull(dt_1st_dx_t2dm)) a
left join (select patid from ac_t2dm_trt_bas_bol where ((isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) and (n_bolus>1 or n_basal>1 or (n_bolus=1 and n_basal=1 )))) b on a.patid=b.patid
left join (select patid from ty44_t2dm_trt where ((isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal))) and (n_glp1>1 or n_mounjaro>1 or (n_glp1=1 and n_mounjaro=1))) c on a.patid=c.patid
;

select * from ty44_t2dm_summary_least2;

-- COMMAND ----------

drop table if exists ty44_t2dm_summary_least2;

create table ty44_t2dm_summary_least2 as
select count(distinct a.patid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.patid) as n_nasal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
from (select patid, dt_1st_dx_t2dm from ty44_t2dm_trt where isnotnull(dt_1st_dx_t2dm)) a
left join (select patid from ty44_t2dm_trt where ((isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus)) and (n_bolus>1 or n_basal>1 or (n_bolus=1 and n_basal=1 )))) b on a.patid=b.patid
left join (select patid from ty44_t2dm_trt where ((isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal))) and (n_glp1>1 or n_mounjaro>1 or (n_glp1=1 and n_mounjaro=1))) c on a.patid=c.patid
;

select * from ty44_t2dm_summary_least2;


-- COMMAND ----------

drop table if exists ty44_t2dm_summary_least2;

create table ty44_t2dm_summary_least2 as
select count(distinct a.patid) as n_t2dm, min(a.dt_1st_dx_t2dm) as dt_t2dm_start, max(a.dt_1st_dx_t2dm) as dt_t2dm_stop, count(distinct b.patid) as n_nasal_bolus
, 100*count(distinct b.patid)/count(distinct a.patid) as pct_basal_bolus, count(distinct c.patid) as n_glp1_mounjaro, 100*count(distinct c.patid)/count(distinct b.patid) as pct_glp1_mounjaro
from (select patid, dt_1st_dx_t2dm from ty44_t2dm_trt where isnotnull(dt_1st_dx_t2dm)) a
left join (select patid from ty44_t2dm_trt where ((isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_bolus)) and (n_bolus>1 or n_basal>1 or (n_bolus=1 and n_basal=1 )))) b on a.patid=b.patid
left join (select patid from ty44_t2dm_trt where ((isnotnull(dt_1st_rx_glp1) or isnotnull(dt_1st_rx_mounjaro)) and (isnotnull(dt_1st_rx_basal) or isnotnull(dt_1st_rx_basal)) and (n_glp1>1 or n_mounjaro>1))) c on a.patid=c.patid
;

select * from ty44_t2dm_summary_least2;


-- COMMAND ----------

select DIAG from ty41_dx_t1dm
group by DIAG
order by DIAG;


-- COMMAND ----------

create or replace table ac_rx_anti_dm_bas_bol_GLP_GIP_1 as
select distinct PTID, 
case when rx_typ in ('Basal','Bolus') in 'Basal-Basal'
when rx_typ in ('GLP1') and lcase(brnd_nm) not like '%mounjaro%' then 'GLP1'
when lcase(brnd_nm) like '%mounjaro%' end as Drug_flag from ac_rx_anti_dm_bas_bol_GLP_GIP
order by patid, fill_dt;

select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP_1
order by patid, fill_dt;
