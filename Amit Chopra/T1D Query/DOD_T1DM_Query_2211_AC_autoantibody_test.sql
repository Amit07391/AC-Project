-- Databricks notebook source
----------Import SES Medical Diagnosis records---------

-- drop table if exists ac_dod_2301_med_diag;

-- create table ac_dod_2301_med_diag using delta location 'dbfs:/mnt/optumclin/202301/ontology/base/dod/Medical Diagnosis';

-- select * from ac_dod_2301_med_diag;


-- drop table if exists ac_dod_2211_med_claim;

-- create table ac_dod_2211_med_claim using delta location '/mnt/optumclin/202212/ontology/base/dod/Medical Claims';

-- select * from ac_dod_2211_med_claim;


-- drop table if exists ac_dod_2211_lab_claim;

-- create table ac_dod_2211_lab_claim using delta location '/mnt/optumclin/202212/ontology/base/dod/Lab Results';

-- select * from ac_dod_2211_lab_claim;


-- drop table if exists ac_dod_2211_mem_conti;

-- create table ac_dod_2211_mem_conti using delta location '/mnt/optumclin/202212/ontology/base/dod/Member Continuous Enrollment';

-- select * from ac_dod_2211_mem_conti;

-- drop table if exists ac_dod_2211_mem_enrol;

-- create table ac_dod_2211_mem_enrol using delta location '/mnt/optumclin/202212/ontology/base/dod/Member Enrollment';

-- select * from ac_dod_2211_mem_enrol;


drop table if exists ac_Taxonomy_lu_spec;

create table ac_Taxonomy_lu_spec using delta location '/dbfs/FileStore/tables/sg_codes/TAXONOMY_lookup_table.csv';

select * from ac_Taxonomy_lu_spec;



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

drop table if exists ac_pr_2211_islet;

create table ac_pr_2211_islet as
select distinct a.patid, a.fst_dt, a.proc_cd, a.prov, a.provcat, b.eligeff, b.eligend, b.gdr_cd, b.race, b.yrdob, b.state, dense_rank() over (partition by a.patid order by a.fst_dt, a.proc_cd) as rank
from ac_dod_2211_med_claim a
inner join ac_dod_2211_mem_enrol b on a.patid=b.patid
where proc_cd in ('86341','86337')
order by a.patid, fst_dt
;

select * from ac_pr_2211_islet
where patid ='33036730258';

-- COMMAND ----------

drop table if exists ac_med_clm_t1dm_antibdy_index;

create table ac_med_clm_t1dm_antibdy_index as
select distinct *, dense_rank() over (partition by patid order by fst_dt) as rank
from (select distinct patid, fst_dt from ac_pr_2211_islet 
      )

order by patid, fst_dt
;

select * from ac_med_clm_t1dm_antibdy_index;


-- COMMAND ----------

-- MAGIC %md #### Checking first test only

-- COMMAND ----------

create or replace table ac_pr_2211_islet_first_tst as
select distinct a.*, c.fst_dt as indx_date_antibdy_tst from ac_pr_2211_islet a
inner join (select * from ac_med_clm_t1dm_antibdy_index where rank=1) c on a.patid=c.patid and a.fst_dt=c.fst_dt;

select distinct * from ac_pr_2211_islet_first_tst
order by patid, fst_dt;

-- select count(distinct patid) as cnts from ac_pr_2211_islet_first_tst;

-- COMMAND ----------

-- MAGIC %md #### removing duplicates

-- COMMAND ----------

create or replace table ac_cdm_t1dm_antibdy_demo_max_dt as
select distinct patid, max(eligend) as max_dt from 
(select distinct patid, year(fst_dt) as yr, eligend from ac_pr_2211_islet)
group by 1;

select distinct * from ac_cdm_t1dm_antibdy_demo_max_dt;

-- COMMAND ----------

create or replace table ac_pr_2211_islet_demo as
select distinct a.patid, a.gdr_cd, a.race, a.yrdob, a.state,
c.fst_dt as indx_date_antibdy_tst
from ac_pr_2211_islet a left join ac_cdm_t1dm_antibdy_demo_max_dt b on a.patid=b.patid and a.eligend = b.max_dt
left join (select * from ac_med_clm_t1dm_antibdy_index where rank=1) c on a.patid=c.patid;

-- select distinct patid, gdr_cd from ac_pr_2211_islet_demo
-- group by patid, gdr_cd
-- having count(*) >1 
-- ;

-- COMMAND ----------

select count(distinct patid) from ac_pr_2211_islet_demo;

-- COMMAND ----------



-- COMMAND ----------

create or replace table ac_pr_2211_islet_1 as
select distinct a.*, b.gdr_cd as gender, b.race as race_new, b.yrdob as DOB, b.state as State_new,
c.fst_dt as indx_date_antibdy_tst
from ac_pr_2211_islet a
inner join ac_pr_2211_islet_demo b on a.patid=b.patid
left join (select * from ac_med_clm_t1dm_antibdy_index where rank=1) c on a.patid=c.patid
order by patid, fst_dt;


-- COMMAND ----------

select distinct  * from ac_pr_2211_islet_1

where patid='33113113678'


-- COMMAND ----------

create or replace table ac_pr_2211_islet_tsts as
select distinct patid, year(fst_dt) as yr, count(distinct fst_dt) as n_tsts from ac_pr_2211_islet_1
group by 1,2
order by 1,2;

select distinct * from ac_pr_2211_islet_tsts
order by 1,2;

select yr, sum(n_tsts) as tsts from ac_pr_2211_islet_tsts
group by 1
order by 1;

-- COMMAND ----------

-- create or replace table ac_pr_2211_islet_tsts_demo as
-- select distinct patid, gender, race_new, state_new, dob, case when year(indx_date_antibdy_tst)-dob<18 and isnotnull(year(indx_date_antibdy_tst)-dob) then 'Age  < 18'
--        when year(indx_date_antibdy_tst)-dob>=18 and year(indx_date_antibdy_tst)-dob<41 then 'Age 18 - 40'
--        when year(indx_date_antibdy_tst)-dob>=41 and year(indx_date_antibdy_tst)-dob<65 then 'Age 41 - 64'
--        when year(indx_date_antibdy_tst)-dob>=65  then 'Age 65+'
--        else null end as age_grp, count(distinct fst_dt) as n_tsts from ac_pr_2211_islet_1
-- group by 1,2,3,4,5,6
-- order by 1;

select distinct * from ac_pr_2211_islet_tsts_demo
-- where patid='33113113678'
order by patid;

-- COMMAND ----------

 select gender, sum(n_tsts) as tsts from ac_pr_2211_islet_tsts_demo
 group by 1;  
 
  select race_new, sum(n_tsts) as tsts from ac_pr_2211_islet_tsts_demo
 group by 1;  
 
  select state_new, sum(n_tsts) as tsts from ac_pr_2211_islet_tsts_demo
 group by 1;  
 
 select distinct age_grp, sum(n_tsts) as tsts from ac_pr_2211_islet_tsts_demo
       group by 1;

-- COMMAND ----------

-- MAGIC %md #### Distinct patients

-- COMMAND ----------

 select gdr_cd, count(distinct patid) as pts from ac_pr_2211_islet_demo
 group by 1;  
 
  select race, count(distinct patid) as pts from ac_pr_2211_islet_demo
 group by 1;  
 
  select state, count(distinct patid) as pts from ac_pr_2211_islet_demo
 group by 1;  
 
--  select year(indx_date_antibdy_tst), count(distinct patid) as pts from ac_pr_2211_islet
--  group by 1
--  order by 1;  
 
-- select distinct case when year(indx_date_antibdy_tst)-yrdob<18 and isnotnull(year(indx_date_antibdy_tst)-yrdob) then 'Age  < 18'
--        when year(indx_date_antibdy_tst)-yrdob>=18 and year(indx_date_antibdy_tst)-yrdob<41 then 'Age 18 - 40'
--        when year(indx_date_antibdy_tst)-yrdob>=41 and year(indx_date_antibdy_tst)-yrdob<65 then 'Age 41 - 64'
--        when year(indx_date_antibdy_tst)-yrdob>=65  then 'Age 65+'
--        else null end as age_grp_t1dm, count(distinct patid) from ac_pr_2211_islet_demo
--        group by 1;

-- COMMAND ----------

select distinct fst_dt from ac_pr_2211_islet_1
where year(indx_date_antibdy_tst)=2020
order by fst_dt;

-- COMMAND ----------

-- drop table if exists ac_cdm_2211_prov;

-- create table ac_cdm_2211_prov using delta location '/mnt/optumclin/202212/ontology/base/dod/Provider';

-- select * from ac_cdm_2211_prov;

drop table if exists ac_cdm_2211_prov_brdg;

create table ac_cdm_2211_prov_brdg using delta location '/mnt/optumclin/202212/ontology/base/dod/Provider Bridge';

select * from ac_cdm_2211_prov_brdg;



-- COMMAND ----------

describe table ac_pr_2211_islet

-- COMMAND ----------


DROP TABLE IF EXISTS ac_pr_2211_islet_prov_spec;
CREATE TABLE ac_pr_2211_islet_prov_spec AS
SELECT DISTINCT A.*, D.CRED_TYPE,D.TAXONOMY1, G.DESCRIPTION AS SPECIALITY_CARE FROM ac_pr_2211_islet_first_tst AS A
LEFT JOIN ac_cdm_2211_prov_brdg E ON A.prov = E.PROV
LEFT JOIN ac_cdm_2211_prov D ON E.PROV_UNIQUE = D.PROV_UNIQUE
LEFT JOIN sg_precriber_tax_only AS G ON D.TAXONOMY1 = G.TAXONOMY;

select * from ac_pr_2211_islet_prov_spec;


-- COMMAND ----------

select SPECIALITY_CARE, count(distinct patid) as cnts from ac_pr_2211_islet_prov_spec group by 1 order by 1;

-- COMMAND ----------


DROP TABLE IF EXISTS ac_pr_2211_islet_prov_spec_tsts;
CREATE TABLE ac_pr_2211_islet_prov_spec_tsts AS
SELECT DISTINCT A.*, D.CRED_TYPE,D.TAXONOMY1, G.DESCRIPTION AS SPECIALITY_CARE FROM ac_pr_2211_islet_1 AS A
LEFT JOIN ac_cdm_2211_prov_brdg E ON A.prov = E.PROV
LEFT JOIN ac_cdm_2211_prov D ON E.PROV_UNIQUE = D.PROV_UNIQUE
LEFT JOIN sg_precriber_tax_only AS G ON D.TAXONOMY1 = G.TAXONOMY;

select * from ac_pr_2211_islet_prov_spec_tsts;


-- COMMAND ----------

create or replace table ac_pr_2211_islet_prov_spec_tsts_1 as
select distinct patid, SPECIALITY_CARE, count(distinct fst_dt) as n_tsts from ac_pr_2211_islet_prov_spec_tsts
group by 1,2
order by 1;

select distinct * from ac_pr_2211_islet_prov_spec_tsts_1
order by patid, 2;

-- COMMAND ----------

select SPECIALITY_CARE, sum(n_tsts)as n_test from ac_pr_2211_islet_prov_spec_tsts_1 group by 1 order by 1;
