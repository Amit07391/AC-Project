-- Databricks notebook source
----------Import SES Medical Diagnosis records---------

-- drop table if exists ac_dod_2303_med_diag;

-- create table ac_dod_2303_med_diag using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Medical Diagnosis';

-- select * from ac_dod_2303_med_diag;
-- select max(fst_dt) from ac_dod_2303_med_diag;


-- drop table if exists ac_dod_2303_med_claims;

-- create table ac_dod_2303_med_claims using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Medical Claims';

-- select * from ac_dod_2303_med_claims;
-- select max(fst_dt) from ac_dod_2303_med_claims;

-- drop table if exists ac_dod_2303_IP_confin;

-- create table ac_dod_2303_IP_confin using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/IP Confinements';

-- select * from ac_dod_2303_IP_confin;
-- select max(fst_dt) from ac_dod_2303_IP_confin;


-- drop table if exists ac_dod_2303_RX_claims;

-- create table ac_dod_2303_RX_claims using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/RX Claims';

-- select * from ac_dod_2303_RX_claims;

-- drop table if exists ac_dod_2303_lu_NDC;

-- create table ac_dod_2303_lu_NDC using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Lookup NDC';

-- select * from ac_dod_2303_lu_NDC;


-- drop table if exists ac_dod_2303_med_proc;

-- create table ac_dod_2303_med_proc using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Medical Procedures';

-- select * from ac_dod_2303_med_proc;


drop table if exists ac_dod_2303_lu_proc;

create table ac_dod_2303_lu_proc using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Lookup Procedure';

select * from ac_dod_2303_lu_proc;




-- COMMAND ----------

-- drop table if exists ac_dod_dx_act_pancrtis_Test;

-- create table ac_dod_dx_act_pancrtis_Test as
-- select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
--                 ,year(fst_dt) as yr_dt
-- from ac_dod_2303_med_diag a
-- where a.fst_dt>='2015-01-01' and (a.diag='5770' or a.diag = 'K8590')
-- order by a.patid, a.fst_dt
-- ;

-- select * from ac_dod_dx_act_pancrtis_Test;

select yr_dt, count(distinct patid) from ac_cdm_dx_ac_pancrtis_indx_1

group by 1

order by 1
;


-- COMMAND ----------

drop table if exists ac_dod_dx_act_pancrtis_1;

create table ac_dod_dx_act_pancrtis_1 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                ,year(fst_dt) as yr_dt
from ac_dod_2303_med_diag a
where a.fst_dt>='2015-01-01' and (a.diag='5770' or a.diag like 'K85%')
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_act_pancrtis_1;

-- COMMAND ----------

select count(distinct patid) from ac_cdm_dx_ac_pancrtis_indx_1; ---249484

select yr_dt, count(distinct patid) from ac_cdm_dx_ac_pancrtis_indx_1
group by 1
order by 1;

-- yr_dt	count(DISTINCT patid)
-- 2015	30828
-- 2016	36560
-- 2017	38756
-- 2018	38365
-- 2019	39701
-- 2020	37301
-- 2021	39887
-- 2022	41051
-- 2023	2764

-- COMMAND ----------

drop table if exists ac_cdm_dx_ac_pancrtis_indx;

create table ac_cdm_dx_ac_pancrtis_indx as
select distinct patid, yr_dt, min(fst_dt) as indx_dt, count(distinct fst_dt) as diag_cnt from ac_dod_dx_act_pancrtis_1
group by 1, 2
order by 1,2

;

select * from ac_cdm_dx_ac_pancrtis_indx;

-- COMMAND ----------

create or replace table ac_cdm_dx_ac_pancrtis_indx_1 as
select distinct * from ac_cdm_dx_ac_pancrtis_indx
where diag_cnt>=2
order by 1;

select distinct * from ac_cdm_dx_ac_pancrtis_indx_1
order by 1;


-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_2 as
select distinct a.*,b.indx_d from ac_dod_dx_act_pancrtis_1 a
inner join ac_cdm_dx_ac_pancrtis_indx_1 b on a.patid=b.patid and a.yr_dt=b.yr_dt
order by patid, fst_dt;

select distinct * from ac_dod_dx_act_pancrtis_2
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_clm as
select distinct a.*, b.ADMIT_CHAN, b.ADMIT_TYPE, b.bill_prov, 
b.charge, b.cob, b.coins, b.conf_id, b.copay, b.deduct, b.lst_dt, b.pos, b.tos_cd
from ac_dod_dx_act_pancrtis_2 a left join ac_dod_2303_med_claims b on a.patid=b.patid and a.clmid=b.clmid and a.fst_dt=b.fst_dt;

select distinct * from ac_dod_dx_act_pancrtis_clm
order by patid, fst_dt;

-- COMMAND ----------

describe table ac_dod_dx_act_pancrtis_clm;

describe table ac_dod_dx_act_panc_IP_conf; 

-- COMMAND ----------

select year(fst_dt) as yr, count(distinct patid) from ac_dod_dx_act_pancrtis_clm
where pos in ('21')
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### joining with inpatient conf table to check patients

-- COMMAND ----------

select yr_Dt, count(distinct a.patid) as cnts from ac_dod_dx_act_pancrtis_2 a
inner join ac_dod_2303_IP_confin b on a.patid=b.patid and a.yr_dt=year(b.admit_date)
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_dod_dx_act_panc_IP_conf as
select distinct * from 
ac_dod_2303_IP_confin
where admit_date>='2015-01-01' and (diag1='5770' or diag1 like 'K85%' or diag2='5770' or diag2 like 'K85%' or diag3='5770' or diag3 like 'K85%')
;

select distinct * from ac_dod_dx_act_panc_IP_conf
order by patid, admit_date;

-- COMMAND ----------

-- select distinct * from ac_dod_dx_act_panc_IP_conf
-- where patid='33003299715'
-- order by patid, admit_date;

select '7 # of patients with cont coverage in 2021' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where   year(eligeff)=2021 and year(eligend)=2021


-- COMMAND ----------

select year(admit_date) as yr, count(distinct patid) from ac_dod_dx_act_panc_IP_conf
where los>=1
group by 1
order by 1;


-- COMMAND ----------

-- MAGIC %md #### length of stay

-- COMMAND ----------

create or replace table ac_dod_dx_act_panc_IP_conf_LOS as
select distinct patid, year(admit_date) as yr, sum(los) as Sum_LOS
from ac_dod_dx_act_panc_IP_conf
group by 1,2
order by 1,2;

select distinct * from ac_dod_dx_act_panc_IP_conf_LOS
where patid='33003299715'
order by patid, yr;

-- COMMAND ----------

select  yr, mean(Sum_LOS) from ac_dod_dx_act_panc_IP_conf_LOS
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Total members

-- COMMAND ----------

drop table if exists ac_dod_2303_mem_cont_enrol;

create table ac_dod_2303_mem_cont_enrol using delta location 'dbfs:/mnt/optumclin/202303/ontology/base/dod/Member Continuous Enrollment';

select * from ac_dod_2303_mem_cont_enrol;



-- COMMAND ----------

create or replace table ac_dod_total_enrolee_2303 as
select '1 # of patients with cont coverage in 2015' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2015-01-01' and '2015-12-31'<=eligend
UNION
select '2 # of patients with cont coverage in 2016' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2016-01-01' and '2016-12-31'<=eligend
UNION
select '3 # of patients with cont coverage in 2017' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2017-01-01' and '2017-12-31'<=eligend
UNION
select '4 # of patients with cont coverage in 2018' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2018-01-01' and '2018-12-31'<=eligend
UNION
select '5 # of patients with cont coverage in 2019' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2019-01-01' and '2019-12-31'<=eligend
UNION
select '6 # of patients with cont coverage in 2020' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2020-01-01' and '2020-12-31'<=eligend
UNION
select '7 # of patients with cont coverage in 2021' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2021-01-01' and '2021-12-31'<=eligend
UNION
select '8 # of patients with cont coverage in 2022' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2022-01-01' and '2022-12-31'<=eligend
UNION
select '9 # of patients with cont coverage in 2023' as cat, count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2023-01-01' and '2023-12-31'<=eligend
order by cat;

select distinct * from ac_dod_total_enrolee_2303
order by cat;


-- COMMAND ----------

-- MAGIC %md #### Incidence population

-- COMMAND ----------

drop table if exists ac_dod_dx_act_pancrtis_full;

create table ac_dod_dx_act_pancrtis_full as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
                ,year(fst_dt) as yr_dt
from ac_dod_2303_med_diag a
where  (a.diag='5770' or a.diag like 'K85%')
order by a.patid, a.fst_dt
;

select * from ac_dod_dx_act_pancrtis_full
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from ac_dod_dx_act_pancrtis_full
where patid='33004241916'
order by fst_dt;

-- COMMAND ----------

-- MAGIC %md #### Indx date

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_full_indx as
select distinct patid, min(fst_dt) as Index_date, count(distinct fst_dt) as diag_cnt from ac_dod_dx_act_pancrtis_full
group by 1
order by 1;

select distinct * from ac_dod_dx_act_pancrtis_full_indx
order by 1;

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_full_indx_1 as
select distinct * from ac_dod_dx_act_pancrtis_full_indx
where diag_cnt>=2
order by patid;

select distinct * from ac_dod_dx_act_pancrtis_full_indx_1
order by patid;

-- COMMAND ----------

-- MAGIC %md #### Joining with med claims

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_incd_clm as
select distinct a.*, b.ADMIT_CHAN, b.ADMIT_TYPE, b.bill_prov, 
b.charge, b.cob, b.coins, b.conf_id, b.copay, b.deduct, b.lst_dt, b.pos, b.tos_cd, c.index_date
from ac_dod_dx_act_pancrtis_full a left join ac_dod_2303_med_claims b on a.patid=b.patid and a.clmid=b.clmid and a.fst_dt=b.fst_dt
inner join ac_dod_dx_act_pancrtis_full_indx_1 c on a.patid=c.patid
where c.index_date>='2015-01-01';

select distinct * from ac_dod_dx_act_pancrtis_incd_clm
order by patid, fst_dt;

-- COMMAND ----------

select year(index_date) as yr, count(distinct patid) from ac_dod_dx_act_pancrtis_incd_clm
where pos in ('21')
group by 1
order by 1

-- COMMAND ----------

-- MAGIC %md #### Method 2 - joining with inp confinement

-- COMMAND ----------

create or replace table ac_dod_dx_act_panc_IP_conf_INC as
select distinct * from 
ac_dod_2303_IP_confin
where (diag1='5770' or diag1 like 'K85%' or diag2='5770' or diag2 like 'K85%' or diag3='5770' or diag3 like 'K85%')
;

select distinct * from ac_dod_dx_act_panc_IP_conf_INC
order by patid, admit_date;

-- COMMAND ----------

create or replace table ac_dod_dx_act_panc_IP_conf_INC_indx as 
select distinct patid, min(admit_date) as index_date from ac_dod_dx_act_panc_IP_conf_INC
group by 1
order by 1;

select distinct * from ac_dod_dx_act_panc_IP_conf_INC_indx
order by 1;


-- COMMAND ----------

select '1 # of patients in 2015' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2015-01-01' and '2015-12-31'
UNION
select '2 # of patients  in 2016' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2016-01-01' and '2016-12-31'
UNION
select '3 # of patients in 2017' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2017-01-01' and '2017-12-31'
UNION
select '4 # of patients in 2018' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2018-01-01' and '2018-12-31'
UNION
select '5 # of patients in 2019' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2019-01-01' and '2019-12-31'
UNION
select '6 # of patients in 2020' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2020-01-01' and '2020-12-31'
UNION
select '7 # of patients in 2021' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2021-01-01' and '2021-12-31'
UNION
select '8 # of patients in 2022' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2022-01-01' and '2022-12-31'
UNION
select '9 # of patients in 2023' as cat, count(distinct patid) as cnts from ac_dod_dx_act_panc_IP_conf_INC_indx
where  Index_date between '2023-01-01' and '2023-12-31'
order by cat;



-- COMMAND ----------

-- MAGIC %md #### Length of stay

-- COMMAND ----------

create or replace table ac_dod_dx_act_panc_IP_conf_INC_1 as
select distinct a.*, year(b.index_date) as yr_indx
from ac_dod_dx_act_panc_IP_conf_INC a
inner join ac_dod_dx_act_panc_IP_conf_INC_indx b on a.patid=b.patid and year(admit_date)=year(index_date)

order by patid, admit_date;

create or replace table ac_dod_dx_act_panc_IP_conf_INC_LOS as
select distinct patid, yr_indx, sum(los) as Sum_LOS
from ac_dod_dx_act_panc_IP_conf_INC_1 a
group by 1,2
order by 1,2;

select yr_indx, mean(sum_los) as mn from ac_dod_dx_act_panc_IP_conf_INC_LOS
group by 1
order by 1;


-- COMMAND ----------

-- MAGIC %md #### Method 3

-- COMMAND ----------

select year(a.index_date) as Year, count(distinct a.patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1 a
inner join ac_dod_2303_IP_confin b on a.patid=b.patid and year(a.index_date)=year(b.admit_date)
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Incidence pop

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_incidnc as
select '1 # of patients in 2015' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2015-01-01' and '2015-12-31'
UNION
select '2 # of patients  in 2016' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2016-01-01' and '2016-12-31'
UNION
select '3 # of patients in 2017' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2017-01-01' and '2017-12-31'
UNION
select '4 # of patients in 2018' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2018-01-01' and '2018-12-31'
UNION
select '5 # of patients in 2019' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2019-01-01' and '2019-12-31'
UNION
select '6 # of patients in 2020' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2020-01-01' and '2020-12-31'
UNION
select '7 # of patients in 2021' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2021-01-01' and '2021-12-31'
UNION
select '8 # of patients in 2022' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2022-01-01' and '2022-12-31'
UNION
select '9 # of patients in 2023' as cat, count(distinct patid) as cnts from ac_dod_dx_act_pancrtis_full_indx_1
where  Index_date between '2023-01-01' and '2023-12-31'
order by cat;

select distinct * from ac_dod_dx_act_pancrtis_incidnc
order by cat;


-- COMMAND ----------

-- MAGIC %md #### Total incidence population

-- COMMAND ----------

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2015-01-01' and '2015-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2014-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2016-01-01' and '2016-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2015-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2017-01-01' and '2017-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2016-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2018-01-01' and '2018-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2017-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2019-01-01' and '2019-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2018-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2020-01-01' and '2020-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2019-12-31' );

-- select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
-- where  eligeff<='2021-01-01' and '2021-12-31'<=eligend
-- and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
-- where fst_dt<='2020-12-31' );


select count(distinct patid) as cnts from ac_dod_2303_mem_cont_enrol
where  eligeff<='2022-01-01' and '2022-12-31'<=eligend
and patid not in (select distinct patid from ac_dod_dx_act_pancrtis_full
where fst_dt<='2021-12-31' );
