-- Databricks notebook source
create or replace table ac_dod_T1D_T2D_All as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(fst_dt) as yr_dt, b.dx_name
from ac_dod_2307_med_diag a
inner join ty00_all_dx_comorb b on a.diag=b.code
where b.dx_name in ('T1DM','T2DM')
order by a.patid, a.fst_dt
;

select distinct * from ac_dod_T1D_T2D_All
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_T1D_all as
select distinct patid, fst_dt, diag from ac_dod_T1D_T2D_All
where dx_name='T1DM'
order by patid, fst_dt;

select distinct * from ac_dod_T1D_all
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_T2D_all as
select distinct patid, fst_dt, diag from ac_dod_T1D_T2D_All
where dx_name='T2DM'
order by patid, fst_dt;

select distinct * from ac_dod_T2D_all
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_t1d_t2d_ratio_all as
select distinct a.patid, count(distinct a.fst_dt) as t1d_cnts, count(distinct b.fst_dt) as t2d_cnts from
(select distinct patid, fst_dt from ac_dod_T1D_all ) a
left join (select distinct patid, fst_dt from ac_dod_T2D_all) b on a.patid=b.patid
group by 1
order by 1;

select distinct * from ac_t1d_t2d_ratio_all
order by 1;

-- COMMAND ----------

create or replace table ac_dod_t1d_eligible_all as
select distinct *, t1d_cnts>2*t2d_cnts as ratio from ac_t1d_t2d_ratio_all
where t1d_cnts>2*t2d_cnts
order by 1;

select distinct * from ac_dod_t1d_eligible_all
order by 1;

-- COMMAND ----------

select count(distinct patid) from ac_dod_t1d_eligible_all

-- COMMAND ----------

create or replace table ac_dod_T1D_All_final as
select distinct a.*, b.yrdob, c.family_id from ac_dod_T1D_all a
left join ac_dod_2307_member_cont_enrol b on a.patid=b.PATID
left join ac_dod_2307_member_enrol c on a.patid=c.patid
where a.patid in (select distinct patid from ac_dod_t1d_eligible_all)
order by a.patid, fst_dt;

select distinct * from ac_dod_T1D_All_final
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_T1D_All_final

-- COMMAND ----------

create or replace table ac_dod_t1d_autoimm_indx as
select distinct patid, min(fst_dt) as T1D_Index_date from
ac_dod_T1D_All_final
group by 1
order by 1;

select distinct * from ac_dod_t1d_autoimm_indx
order by 1;

-- COMMAND ----------

create or replace table ac_dod_all_T1D_autoimm_dx as
select distinct a.patid, a.pat_planid, a.clmid, a.fst_dt, a.diag, a.diag_position, a.icd_flag, a.loc_cd, a.poa
,year(a.fst_dt) as yr_dt, b.T1D_Index_date
from ac_dod_2307_med_diag a
inner join ac_dod_t1d_autoimm_indx b on a.PATID=b.patid
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

select distinct * from ac_dod_all_T1D_autoimm_dx
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_all_T1D_autoimm_dx_2 as
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
from ac_dod_all_T1D_autoimm_dx
order by patid, fst_dt;

select distinct * from ac_dod_all_T1D_autoimm_dx_2
order by patid, fst_dt;

-- COMMAND ----------

select distinct patid, t1d_index_date from ac_dod_t1d_autoimm_indx where year(T1D_Index_date)>=2016

-- COMMAND ----------

select case when year(T1D_Index_date)-b.yrdob>=18 then '18+'
when year(T1D_Index_date)-b.yrdob<18 then 'less than 18' end as Age_flag
, count(distinct a.patid) from ac_dod_t1d_autoimm_indx a 
inner join ac_dod_T1D_All_final b on a.patid=b.patid where year(T1D_Index_date)>=2016
group by 1
order by 1;

-- COMMAND ----------

select dx_flag, count(distinct patid) from ac_dod_all_T1D_autoimm_dx_2
where year(T1D_Index_date)>=2016
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_dod_t1d_autoimm_dx_list as
select distinct a.patid as patid1, min(b.celiac_1st_dt) as celiac_1st_dt, min(c.hypo_1st_dt) as hypo_1st_dt, min(d.grave_1st_dt) as grave_1st_dt, min(e.gastritis_1st_dt) as gastritis_1st_dt, min(f.SLE_1st_dt) as SLE_1st_dt, min(g.Vititligo_1st_dt) as Vititligo_1st_dt, min(h.hepa_1st_dt) as hepa_1st_dt
from (select distinct patid, t1d_index_date from ac_dod_t1d_autoimm_indx where year(T1D_Index_date)>=2016) a
left join (select distinct patid, min(fst_dt) as celiac_1st_dt from ac_dod_all_T1D_autoimm_dx_2 where Dx_flag="Celiac"
group by patid) b on a.patid=b.patid
left join (select distinct patid, min(fst_dt) as hypo_1st_dt from ac_dod_all_T1D_autoimm_dx_2 where Dx_flag="Hypothyroidism" group by patid) c on a.patid=c.patid
left join (select distinct patid, min(fst_dt) as grave_1st_dt from ac_dod_all_T1D_autoimm_dx_2 where Dx_flag="Grave Disease" group by patid) d on a.patid=d.patid
left join (select distinct patid, min(fst_dt) as gastritis_1st_dt from ac_dod_all_T1D_autoimm_dx_2 where Dx_flag="Autoimmune Gastritis" group by patid) e on a.patid=e.patid
left join (select distinct patid, min(fst_dt) as SLE_1st_dt from ac_dod_all_T1D_autoimm_dx_2 where Dx_flag="SLE" group by patid) f on a.patid=f.patid
left join (select distinct patid, min(fst_dt) as Vititligo_1st_dt from ac_dod_all_T1D_autoimm_dx_2 where Dx_flag="Vitiligo" group by patid) g on a.patid=g.patid
left join (select distinct patid, min(fst_dt) as hepa_1st_dt from ac_dod_all_T1D_autoimm_dx_2 where Dx_flag="Autoimmune Hepatitis" group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select distinct * from ac_dod_t1d_autoimm_dx_list
order by patid1;

-- COMMAND ----------

select count(distinct patid) from ac_dod_all_T1D_autoimm_dx_2

-- COMMAND ----------

create or replace table ac_dod_t1d_autoimm_dx_list_2 as
select distinct a.patid,a.T1D_Index_date,year(a.T1D_Index_date) as Year_indx, c.yrdob, b.*  from ac_dod_all_T1D_autoimm_dx_2 a
inner join ac_dod_t1d_autoimm_dx_list b on a.patid=b.patid1
inner join ac_dod_T1D_All_final c on a.patid=c.patid
order by a.patid;

select distinct * from ac_dod_t1d_autoimm_dx_list_2
order by 1;

-- COMMAND ----------

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, patid from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date<gastritis_1st_dt
order by 1,2;

-- COMMAND ----------

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date<hypo_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date>hypo_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date=hypo_1st_dt
group by 1
order by 1,2;

-- COMMAND ----------

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date<gastritis_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date>gastritis_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date=gastritis_1st_dt
group by 1
order by 1,2;

-- COMMAND ----------

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date<grave_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date>grave_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date=grave_1st_dt
group by 1
order by 1,2;

-- COMMAND ----------

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date<SLE_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date>SLE_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date=SLE_1st_dt
group by 1
order by 1,2;

-- COMMAND ----------

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date<Vititligo_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date>Vititligo_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date=Vititligo_1st_dt
group by 1
order by 1,2;

-- COMMAND ----------

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date<hepa_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date>hepa_1st_dt
group by 1
order by 1,2;

select distinct case when year_indx-yrdob>=18 then '18+'
when year_indx-yrdob<18 then 'less than 18' end as Age_flag, count(distinct patid) as pts from ac_dod_t1d_autoimm_dx_list_2
where t1d_index_date=hepa_1st_dt
group by 1
order by 1,2;
