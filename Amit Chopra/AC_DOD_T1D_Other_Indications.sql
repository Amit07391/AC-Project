-- Databricks notebook source

-- drop table if exists ac_dod_2307_med_diag;
-- create table ac_dod_2307_med_diag using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Medical Diagnosis';

-- select distinct * from ac_dod_2307_med_diag;

-- drop table if exists ac_dod_2307_member_cont_enrol;
-- create table ac_dod_2307_member_cont_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Continuous Enrollment';

-- select distinct * from ac_dod_2307_member_cont_enrol;

drop table if exists ac_dod_2307_member_enrol;
create table ac_dod_2307_member_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Enrollment';

select distinct * from ac_dod_2307_member_enrol;

-- COMMAND ----------

create or replace table ac_dod_T1D_T2D_2022 as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(fst_dt) as yr_dt, b.dx_name
from ac_dod_2307_med_diag a
inner join ty00_all_dx_comorb b on a.diag=b.code
where a.fst_dt>='2022-01-01' AND a.fst_dt<='2022-12-31' AND
b.dx_name in ('T1DM','T2DM')
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_T1D_T2D_2022
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_T1D_2022 as
select distinct patid, fst_dt, diag from ac_dod_T1D_T2D_2022
where dx_name='T1DM'
order by patid, fst_dt;

select distinct * from ac_dod_T1D_2022
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_T1D_2022

-- COMMAND ----------

create or replace table ac_dod_T2D_2022 as
select distinct patid, fst_dt, diag from ac_dod_T1D_T2D_2022
where dx_name='T2DM'
order by patid, fst_dt;

select distinct * from ac_dod_T2D_2022
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_t1d_t2d_ratio_2022 as
select distinct a.patid, count(distinct a.fst_dt) as t1d_cnts, count(distinct b.fst_dt) as t2d_cnts from
(select distinct patid, fst_dt from ac_dod_T1D_2022 where fst_dt between '2022-01-01' and '2022-12-31') a
left join (select distinct patid, fst_dt from ac_dod_T2D_2022 where fst_dt between '2022-01-01' and '2022-12-31') b on a.patid=b.patid
group by 1
order by 1;

select distinct * from ac_t1d_t2d_ratio_2022
order by 1;

-- COMMAND ----------

create or replace table ac_dod_t1d_eligible_2022 as
select distinct *, t1d_cnts>2*t2d_cnts as ratio from ac_t1d_t2d_ratio_2022
where t1d_cnts>2*t2d_cnts
order by 1;

select distinct * from ac_dod_t1d_eligible_2022
order by 1;

-- COMMAND ----------

create or replace table ac_dod_T1D_2022_final as
select distinct a.*, b.yrdob, c.FAMILY_ID from ac_dod_T1D_2022 a
left join ac_dod_2307_member_cont_enrol b on a.patid=b.PATID
left join ac_dod_2307_member_enrol c on a.patid=c.patid
where a.patid in (select distinct patid from ac_dod_t1d_eligible_2022)
order by a.patid, fst_dt;

select distinct * from ac_dod_T1D_2022_final
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_T1D_2022_final;

select distinct FAMILY_ID from ac_dod_T1D_2022_final;

-- COMMAND ----------

create or replace table ac_dod_t1d_family_pts as
select distinct a.PATID, a.family_id, a.YRDOB, 2022-a.yrdob as Age from ac_dod_2307_member_enrol a
inner join ac_dod_T1D_2022_final b on a.FAMILY_ID=b.FAMILY_ID
where a.PATID not in (select distinct patid from ac_dod_T1D_2022_final)
order by 1;

select distinct * from ac_dod_t1d_family_pts
order by 1;


-- COMMAND ----------

create or replace table ac_dod_t1d_fam_reltn_pts as
select distinct a.patid, b.patid as fam_patid, b.family_id, b.age from ac_dod_T1D_2022_final a
inner join ac_dod_t1d_family_pts b on a.family_id=b.family_id;

select distinct * from ac_dod_t1d_fam_reltn_pts
order by family_id, patid, fam_patid;

-- COMMAND ----------

select count(distinct family_id) from ac_dod_T1D_2022_final ; -- 140635

select count(distinct family_id) from ac_dod_t1d_family_pts;  -- 53914

select count(distinct family_id) from ac_dod_t1d_fam_reltn_pts;   -- 53914



-- COMMAND ----------

create or replace table ac_dod_t1d_fam_reltn_pts_grp as
select distinct family_id, patid, count(distinct fam_patid) as fam_cnts from ac_dod_t1d_fam_reltn_pts
group by 1,2
order by 1,2;

select distinct * from ac_dod_t1d_fam_reltn_pts_grp
order by 1,2;

-- COMMAND ----------

select count(distinct patid) from ac_dod_t1d_fam_reltn_pts_grp
where fam_cnts>=1;

-- COMMAND ----------

create or replace table ac_dod_t1d_fam_reltn_pts_grp_lt_18 as
select distinct family_id, patid, count(distinct fam_patid) as fam_cnts from ac_dod_t1d_fam_reltn_pts
where age<18
group by 1,2
order by 1,2;

select distinct * from ac_dod_t1d_fam_reltn_pts_grp_lt_18
order by 1,2;

-- COMMAND ----------

select count(distinct patid) from ac_dod_t1d_fam_reltn_pts_grp_lt_18
where fam_cnts>1; -- 7241

select count(distinct patid) from ac_dod_t1d_fam_reltn_pts_grp_lt_18;

-- COMMAND ----------


select count(distinct fam_patid) from ac_dod_t1d_fam_reltn_pts
where age<18;

create or replace table ac_dod_t1d_fam_pts_lt_18 as   
select distinct patid, family_id, fam_patid from ac_dod_t1d_fam_reltn_pts
where age<18;

-- COMMAND ----------

select count(distinct fam_patid) from ac_dod_t1d_fam_pts_lt_18
-- where age<18;

-- COMMAND ----------

create or replace table ac_dod_t1d_family_pts_2 as
select distinct * from ac_dod_t1d_family_pts
where age<18
order by 1;


-- COMMAND ----------

create or replace table ac_dod_T1D_family_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, c.*
from ac_dod_2307_med_diag a
inner join ac_dod_t1d_fam_pts_lt_18 b on a.PATID=b.fam_patid
inner join ty00_all_dx_comorb c on a.diag=c.code
where c.dx_name in ('T1DM') 
-- and a.FST_DT>='2016-01-01'
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_T1D_family_dx
order by patid, fst_dt;


-- COMMAND ----------

select count(distinct patid) from ac_dod_T1D_family_dx

-- COMMAND ----------

create or replace table ac_dod_t1d_dx_fam_pts_final as
select distinct * from ac_dod_t1d_fam_reltn_pts 
where fam_patid in (select distinct patid from ac_dod_T1D_family_dx)
-- where c.fam_cnts>1
order by patid;

select distinct * from ac_dod_t1d_dx_fam_pts_final
order by family_id, patid, fam_patid;

-- COMMAND ----------

select count(distinct patid) from ac_dod_t1d_dx_fam_pts_final; --63

-- select count(distinct fam_patid) from ac_dod_t1d_dx_fam_pts_final; --63

-- COMMAND ----------

select count(distinct patid) from ac_dod_t1d_family_eligible_2016

-- COMMAND ----------

create or replace table ac_dod_t1d_family_elig_indx as
select distinct a.patid, min (a.fst_dt) as t1d_fam_index_date from ac_dod_T1D_family_dx a
inner join ac_dod_t1d_family_eligible_2016 b on a.patid=b.patid
group by 1
order by 1;

select distinct * from ac_dod_t1d_family_elig_indx
order by patid;

-- COMMAND ----------

create or replace table ac_dod_t1d_family_elig_1 as
select distinct a.*, b.t1d_fam_index_date from ac_dod_T1D_family_dx a
inner join ac_dod_t1d_family_elig_indx b on a.patid=b.patid
order by a.patid, a.fst_dt;

select distinct * from ac_dod_t1d_family_elig_1
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_T1D_2022_final_2 as
select distinct *, 2022-yrdob as Age from ac_dod_T1D_2022_final 
order by patid, fst_dt;

select distinct * from ac_dod_T1D_2022_final_2
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_T1D_oth_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, b.Age
from ac_dod_2307_med_diag a
inner join ac_dod_T1D_2022_final_2 b on a.PATID=b.patid
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_T1D_oth_dx
order by patid, fst_dt;


-- COMMAND ----------

select count(distinct patid) from ac_dod_T1D_oth_dx

-- COMMAND ----------

create or replace table ac_dod_T1D_oth_dx_2 as
select distinct *,
case when diag='K900' then 'Celiac'
when diag in ('E063',
'E065',
'E038',
'E069') then 'Hypothyroidism'
when diag in ('E049','E050') then 'Grave Disease'
when diag in ('K2930',
'K2931',
'K2940',
'K2941',
'K2950',
'K2951',
'K2960',
'K2961',
'K2970',
'K2971') then 'Autoimmune Gastritis'
when diag in ('M320',
'M3210',
'M3211',
'M3212',
'M3213',
'M3214',
'M3215',
'M3219',
'M328',
'M329') then 'SLE'
when diag in ('L80',
'H2731',
'H2732',
'H2733',
'H2734',
'H2735',
'H2736',
'H2739') then 'Vitiligo'
when diag in ('K754') then 'Autoimmune Hepatitis'
else 'Others'
end as Dx_flag
from ac_dod_T1D_oth_dx
order by patid, fst_dt;

select distinct * from ac_dod_T1D_oth_dx_2
order by patid, fst_dt;

-- COMMAND ----------

select case when Age>=18 then '18+'
when Age<18 then 'less than 18' end as Age_flag, dx_flag, count(distinct patid) as cnts from ac_dod_T1D_oth_dx_2
group by 1,2
order by 1,2;

-- COMMAND ----------

select count(distinct patid) from ac_dod_T1D_oth_dx_2

-- COMMAND ----------

select case when Age>=18 then '18+'
when Age<18 then 'less than 18' end as Age_flag, count(distinct patid) as cnts from ac_dod_T1D_oth_dx_2
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_dod_all_T1D_fam_autoimm_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, b.yrdob, b.t1d_fam_index_date
from ac_dod_2307_med_diag a
inner join ac_dod_t1d_family_elig_1 b on a.PATID=b.patid
where a.diag in ('K900','E063',
'E065',
'E038',
'E069','E049','E050','K2930',
'K2931',
'K2940',
'K2941',
'K2950',
'K2951',
'K2960',
'K2961',
'K2970',
'K2971','M320',
'M3210',
'M3211',
'M3212',
'M3213',
'M3214',
'M3215',
'M3219',
'M328',
'M329','L80',
'H2731',
'H2732',
'H2733',
'H2734',
'H2735',
'H2736',
'H2739','K754')
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_all_T1D_fam_autoimm_dx
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_all_T1D_fam_autoimm_dx_2 as
select distinct *,
case when diag='K900' then 'Celiac'
when diag in ('E063',
'E065',
'E038',
'E069') then 'Hypothyroidism'
when diag in ('E049','E050') then 'Grave Disease'
when diag in ('K2930',
'K2931',
'K2940',
'K2941',
'K2950',
'K2951',
'K2960',
'K2961',
'K2970',
'K2971') then 'Autoimmune Gastritis'
when diag in ('M320',
'M3210',
'M3211',
'M3212',
'M3213',
'M3214',
'M3215',
'M3219',
'M328',
'M329') then 'SLE'
when diag in ('L80',
'H2731',
'H2732',
'H2733',
'H2734',
'H2735',
'H2736',
'H2739') then 'Vitiligo'
when diag in ('K754') then 'Autoimmune Hepatitis'
else 'Others'
end as Dx_flag
from ac_dod_all_T1D_fam_autoimm_dx
order by patid, fst_dt;

select distinct * from ac_dod_all_T1D_fam_autoimm_dx_2
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_all_T1D_fam_autoimm_dx_2

-- COMMAND ----------

select case when year(t1d_fam_index_date)-yrdob>=18 then '18+'
when year(t1d_fam_index_date)-yrdob<18 then 'less than 18' end as Age_flag,dx_flag,  count(distinct patid) as cnts from ac_dod_all_T1D_fam_autoimm_dx_2
group by 1,2
order by 1,2;
