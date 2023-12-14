-- Databricks notebook source
create or replace table ac_dod_t1d_autoimm_fam_1 as
select distinct a.*, b.family_id from opt_diabetes_autoimm_all a
left join ac_dod_2307_member_enrol b on a.patid=b.PATID
order by a.patid;

select distinct * from ac_dod_t1d_autoimm_fam_1
order by patid;


-- COMMAND ----------

create or replace table ac_dod_t1d_autoimm_fam_2 as
select distinct a.PATID, a.family_id, b.diag_grp from ac_dod_2307_member_enrol a
inner join ac_dod_t1d_autoimm_fam_1 b on a.FAMILY_ID=b.FAMILY_ID
where a.PATID not in (select distinct patid from ac_dod_t1d_autoimm_fam_1)
order by 1;

select distinct * from ac_dod_t1d_autoimm_fam_2
order by 1;


-- COMMAND ----------

create or replace table ac_dod_t1d_autoimm_fam_3 as
select distinct a.patid,a.diag_grp, b.patid as fam_patid, b.family_id from ac_dod_t1d_autoimm_fam_1 a
inner join ac_dod_t1d_autoimm_fam_2 b on a.family_id=b.family_id;

select distinct * from ac_dod_t1d_autoimm_fam_3
order by family_id, patid, fam_patid;

-- COMMAND ----------

select age, a.diag_grp, count(distinct a.patid) from ac_dod_t1d_autoimm_fam_3 a
left join ac_dod_t1d_autoimm_fam_1 b on a.patid=b.patid
group by 1,2
order by 1,2; 

select age, a.diag_grp, count(distinct a.fam_patid) from ac_dod_t1d_autoimm_fam_3 a
left join ac_dod_t1d_autoimm_fam_1 b on a.patid=b.patid
group by 1,2
order by 1,2;

-- COMMAND ----------

create or replace table ac_dod_t1d_autoimm_fam_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, c.*
from ac_dod_2307_med_diag a
inner join ac_dod_t1d_autoimm_fam_3 b on a.PATID=b.fam_patid
inner join ty00_all_dx_comorb c on a.diag=c.code
where c.dx_name in ('T1DM') 
and a.FST_DT>='2016-01-01'
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_t1d_autoimm_fam_dx
order by patid, fst_dt;


-- COMMAND ----------

select count(distinct patid) from ac_dod_t1d_autoimm_fam_dx

-- COMMAND ----------

create or replace table ac_dod_t1d_autoimm_fam_4 as
select distinct a.*, b.age from ac_dod_t1d_autoimm_fam_3 a
left join ac_dod_t1d_autoimm_fam_1 b on a.patid=b.patid
where fam_patid in (select distinct patid from ac_dod_t1d_autoimm_fam_dx)
order by patid;

select distinct * from ac_dod_t1d_autoimm_fam_4
order by patid;


-- COMMAND ----------

select age, diag_grp, count(distinct patid) from ac_dod_t1d_autoimm_fam_4
group by 1,2
order by 1,2;

select age, count(distinct patid) from ac_dod_t1d_autoimm_fam_4
group by 1
order by 1;

-- COMMAND ----------

select age, count(distinct patid) from optum_t1d_q_idx3
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_dod_t1d_only_fam_1 as
select distinct a.*, b.family_id from optum_t1d_q_idx3 a
left join ac_dod_2307_member_enrol b on a.patid=b.PATID
order by a.patid;

select distinct * from ac_dod_t1d_only_fam_1
order by patid;


-- COMMAND ----------

create or replace table ac_dod_t1d_only_fam_2 as
select distinct a.PATID, a.family_id, b.age from ac_dod_2307_member_enrol a
inner join ac_dod_t1d_only_fam_1 b on a.FAMILY_ID=b.FAMILY_ID
where a.PATID not in (select distinct patid from ac_dod_t1d_only_fam_1)
order by 1;

select distinct * from ac_dod_t1d_only_fam_2
order by 1;


-- COMMAND ----------

create or replace table ac_dod_t1d_only_fam_3 as
select distinct a.patid,a.age, b.patid as fam_patid, b.family_id from ac_dod_t1d_only_fam_1 a
inner join ac_dod_t1d_only_fam_2 b on a.family_id=b.family_id;

select distinct * from ac_dod_t1d_only_fam_3
order by family_id, patid, fam_patid;

-- COMMAND ----------

select b.age, count(distinct a.patid) from ac_dod_t1d_only_fam_3 a
left join ac_dod_t1d_only_fam_1 b on a.patid=b.patid
group by 1
order by 1;

select b.age,count(distinct a.fam_patid) from ac_dod_t1d_only_fam_3 a
left join ac_dod_t1d_only_fam_1 b on a.patid=b.patid
group by 1
order by 1;

-- select  count(distinct patid) from ac_dod_t1d_only_fam_3;

-- COMMAND ----------

create or replace table ac_dod_t1d_only_fam_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, c.*
from ac_dod_2307_med_diag a
inner join ac_dod_t1d_only_fam_3 b on a.PATID=b.fam_patid
inner join ty00_all_dx_comorb c on a.diag=c.code
where c.dx_name in ('T1DM') 
and a.FST_DT>='2016-01-01'
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_t1d_only_fam_dx
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_t1d_only_fam_4 as
select distinct a.* from ac_dod_t1d_only_fam_3 a
left join ac_dod_t1d_only_fam_1 b on a.patid=b.patid
where fam_patid in (select distinct patid from ac_dod_t1d_only_fam_dx)
order by a.patid;

select distinct * from ac_dod_t1d_only_fam_4
order by patid;


-- COMMAND ----------

select age, count(distinct patid) from ac_dod_t1d_only_fam_4
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### All T1D and Autoimmune patients from 2016

-- COMMAND ----------

select distinct * from SA_opt_diabetes_autoimm;

select count(distinct patid) from SA_opt_diabetes_autoimm;  -- 42572
select count(distinct patid) from SA_opt_diabetes_autoimm
where yrdob is null;  -- 42572

-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_autoimm_fam_1 as
select distinct a.patid,a.diag_grp, year(a.t1d_idx)- b.yrdob as Age, b.family_id from SA_opt_diabetes_autoimm a
left join ac_dod_2307_member_enrol b on a.patid=b.PATID
order by a.patid;

select distinct * from ac_dod_2016_23_t1d_autoimm_fam_1
order by patid;


-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_autoimm_fam_2 as
select distinct a.PATID, a.family_id, b.diag_grp from ac_dod_2307_member_enrol a
inner join ac_dod_2016_23_t1d_autoimm_fam_1 b on a.FAMILY_ID=b.FAMILY_ID
where a.PATID not in (select distinct patid from ac_dod_2016_23_t1d_autoimm_fam_1)
order by 1;

select distinct * from ac_dod_2016_23_t1d_autoimm_fam_2
order by 1;


-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_autoimm_fam_3 as
select distinct a.patid,a.age, b.patid as fam_patid, b.family_id from ac_dod_2016_23_t1d_autoimm_fam_1 a
inner join ac_dod_2016_23_t1d_autoimm_fam_2 b on a.family_id=b.family_id;

select distinct * from ac_dod_2016_23_t1d_autoimm_fam_3
order by family_id, patid, fam_patid;

-- COMMAND ----------

select case when a.age>=18 then 'Adult'
when a.age<18 then 'Pediatric' end as Age_grp,b.diag_grp, count(distinct a.patid) from ac_dod_2016_23_t1d_autoimm_fam_3 a
left join ac_dod_2016_23_t1d_autoimm_fam_1 b on a.patid=b.patid
group by 1,2
order by 1,2;

select case when a.age>=18 then 'Adult'
when a.age<18 then 'Pediatric' end as Age_grp,b.diag_grp, count(distinct a.fam_patid) from ac_dod_2016_23_t1d_autoimm_fam_3 a
left join ac_dod_2016_23_t1d_autoimm_fam_1 b on a.patid=b.patid
group by 1,2
order by 1,2;

-- select  count(distinct patid) from ac_dod_t1d_only_fam_3;

-- COMMAND ----------

create or replace table ac_dod_t1d_2016_23_fam_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, c.*
from ac_dod_2307_med_diag a
inner join ac_dod_2016_23_t1d_autoimm_fam_3 b on a.PATID=b.fam_patid
inner join ty00_all_dx_comorb c on a.diag=c.code
where c.dx_name in ('T1DM') 
and a.FST_DT>='2016-01-01'
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_t1d_2016_23_fam_dx
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_autoimm_fam_4 as
select distinct a.*, b.diag_grp from ac_dod_2016_23_t1d_autoimm_fam_3 a
left join ac_dod_2016_23_t1d_autoimm_fam_1 b on a.patid=b.patid
where fam_patid in (select distinct patid from ac_dod_t1d_2016_23_fam_dx)
order by a.patid;

select distinct * from ac_dod_2016_23_t1d_autoimm_fam_4
order by patid;


-- COMMAND ----------

select case when age>=18 then 'Adult'
when age<18 then 'Pediatric' end as Age_grp,diag_grp, count(distinct patid) from ac_dod_2016_23_t1d_autoimm_fam_4
group by 1,2
order by 1,2;

-- COMMAND ----------

-- MAGIC %md ###T1D from 2016 onwards only

-- COMMAND ----------

select count(distinct patid) from SA_optum_t1d_q_idx; -- 172124

select distinct * from SA_optum_t1d_q_idx; 

select count(distinct patid) from SA_optum_t1d_q_idx
where yrdob is null;

-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_only_fam_1 as
select distinct a.patid, year(a.t1d_idx)- b.yrdob as Age, b.family_id from SA_optum_t1d_q_idx a
left join ac_dod_2307_member_enrol b on a.patid=b.PATID
order by a.patid;

select distinct * from ac_dod_2016_23_t1d_only_fam_1
order by patid;


-- COMMAND ----------

select patid, age from ac_dod_2016_23_t1d_only_fam_1
where age is null;

-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_only_fam_2 as
select distinct a.PATID, a.family_id from ac_dod_2307_member_enrol a
inner join ac_dod_2016_23_t1d_only_fam_1 b on a.FAMILY_ID=b.FAMILY_ID
where a.PATID not in (select distinct patid from ac_dod_2016_23_t1d_only_fam_1)
order by 1;

select distinct * from ac_dod_2016_23_t1d_only_fam_2
order by 1;


-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_only_fam_3 as
select distinct a.patid,a.age, b.patid as fam_patid, b.family_id from ac_dod_2016_23_t1d_only_fam_1 a
inner join ac_dod_2016_23_t1d_only_fam_2 b on a.family_id=b.family_id;

select distinct * from ac_dod_2016_23_t1d_only_fam_3
order by family_id, patid, fam_patid;

-- COMMAND ----------

select patid, age from ac_dod_2016_23_t1d_only_fam_3
where age is null;

-- COMMAND ----------

select case when age>=18 then 'Adult'
when age<18 then 'Pediatric' end as Age_grp, count(distinct patid) from ac_dod_2016_23_t1d_only_fam_3
group by 1
order by 1;

select case when age>=18 then 'Adult'
when age<18 then 'Pediatric' end as Age_grp, count(distinct fam_patid) from ac_dod_2016_23_t1d_only_fam_3
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_dod_t1d_only_2016_23_fam_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, c.*
from ac_dod_2307_med_diag a
inner join ac_dod_2016_23_t1d_only_fam_3 b on a.PATID=b.fam_patid
inner join ty00_all_dx_comorb c on a.diag=c.code
where c.dx_name in ('T1DM') 
and a.FST_DT>='2016-01-01'
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_t1d_only_2016_23_fam_dx
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_2016_23_t1d_only_fam_4 as
select distinct a.* from ac_dod_2016_23_t1d_only_fam_3 a
left join ac_dod_2016_23_t1d_only_fam_1 b on a.patid=b.patid
where fam_patid in (select distinct patid from ac_dod_t1d_only_2016_23_fam_dx)
order by a.patid;

select distinct * from ac_dod_2016_23_t1d_only_fam_4
order by patid;


-- COMMAND ----------

select case when age>=18 then 'Adult'
when age<18 then 'Pediatric' end as Age_grp, count(distinct patid) from ac_dod_2016_23_t1d_only_fam_4
group by 1
order by 1;
