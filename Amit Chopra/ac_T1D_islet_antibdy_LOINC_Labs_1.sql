-- Databricks notebook source
create or replace table ac_dod_t1d_antibody_lab_1 as
select distinct * from ac_dod_2301_lab_claim
where loinc_cd in ('56718-0',
'56546-5',
'56540-8',
'13927-9',
'76651-9');

select distinct * from ac_dod_t1d_antibody_lab_1
order by patid, fst_dt;

-- COMMAND ----------

select count (*) from ac_dod_t1d_antibody_lab_1

-- COMMAND ----------

select distinct rslt_nbr, rslt_txt, rslt_unit_nm from ac_dod_t1d_antibody_lab_1
where loinc_cd='13927-9' 

-- COMMAND ----------

select distinct rslt_nbr from ac_dod_t1d_antibody_lab_1

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_2 as
select distinct *, case when rslt_unit_nm='IU/L' then cast(rslt_nbr as float)/1000 
else cast(rslt_nbr as float) end as Result_value 
from ac_dod_t1d_antibody_lab_1
-- where rslt_unit_nm not in ('null',
-- 'JDF UNITS',
-- 'JDF Units',
-- 'N',
-- '{titer}',
-- '%',
-- 'ML',
-- 'JDF units',
-- 'nmol/L',
-- 'NULL',
-- 'units',
-- 'JDF_UNITS')
order by patid, fst_dt;


-- COMMAND ----------

select distinct * from ac_dod_t1d_antibody_lab_2


-- COMMAND ----------

-- MAGIC %md #### Also Checking the max test value for QC purpose

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_max_value as
select distinct patid, loinc_cd, max(result_value) as Max_value from ac_dod_t1d_antibody_lab_2
group by 1,2
order by 1,2;

select distinct * from ac_dod_t1d_antibody_lab_max_value;

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_3 as
select distinct a.*, b.max_value from ac_dod_t1d_antibody_lab_2 a
inner join ac_dod_t1d_antibody_lab_max_value b on a.patid=b.patid and a.loinc_cd=b.loinc_cd
order by a.patid, fst_dt;

select distinct * from ac_dod_t1d_antibody_lab_3
where patid='33005784080';



-- COMMAND ----------

select distinct loinc_cd, rslt_nbr, rslt_txt, rslt_unit_nm from ac_dod_t1d_antibody_lab_3
where rslt_nbr is null or rslt_nbr=0;

-- COMMAND ----------

-- MAGIC %md #### Using both max and any test value, but using any test value for the final results. Did some manual work to include text results

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_4 as
select distinct *,
case when loinc_cd='56718-0' and (result_value>=5.4 or rslt_txt in ('>350.0','>350.0','>221.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='56546-5' and result_value>=0.4 and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='56540-8' and (result_value>=5 or rslt_txt in ('>126.0',
'>250.0',
'>134.0',
'>250',
'>119.0',
'>154.0',
'>120',
'>250',
'>132.0',
'>121.0',
'>250.0',
'>120',
'>105.0',
'>114.7',
'>146.0',
'>119.0',
'>120',
'>108.0',
'>142.0',
'>142.0',
'>129.0',
'>147.0',
'>128.0',
'>148.9',
'>250.0',
'>183.0',
'>116.0',
'>115.0',
'>118.0',
'>141.0',
'>146.0',
'>114.7',
'>174.0',
'>159.0',
'>160.0',
'>181.0',
'>163.0',
'>157.0',
'>162.0',
'>148.0',
'>168.0',
'>144.0',
'>149.0',
'>172.0',
'>130.0',
'>149.8',
'>113.0',
'>161.0',
'>104.0',
'>139.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='76651-9' and (result_value>=15 or rslt_txt in ('>500.0',
'>500')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'

end as Flag_Result,

case when loinc_cd='56718-0' and (max_value>=5.4 or rslt_txt in ('>350.0','>350.0','>221.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS')  then 'Positive'
when loinc_cd='56546-5' and max_value>=0.4 and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS')  then 'Positive'
when loinc_cd='56540-8' and (max_value>=5 or rslt_txt in ('>126.0',
'>250.0',
'>134.0',
'>250',
'>119.0',
'>154.0',
'>120',
'>250',
'>132.0',
'>121.0',
'>250.0',
'>114.7',
'>120',
'>105.0',
'>146.0',
'>119.0',
'>120',
'>108.0',
'>142.0',
'>142.0',
'>129.0',
'>147.0',
'>128.0',
'>148.9',
'>250.0',
'>183.0',
'>116.0',
'>115.0',
'>118.0',
'>141.0',
'>146.0',
'>114.7',
'>174.0',
'>159.0',
'>160.0',
'>181.0',
'>163.0',
'>157.0',
'>162.0',
'>148.0',
'>168.0',
'>144.0',
'>149.0',
'>172.0',
'>130.0',
'>149.8',
'>113.0',
'>161.0',
'>104.0',
'>139.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='76651-9' and ( max_value>=15 or rslt_txt in ('>500.0',
'>500')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS')  then 'Positive' end as Flag_Result_max_value
from ac_dod_t1d_antibody_lab_3
order by patid, fst_dt;

select distinct * from ac_dod_t1d_antibody_lab_4
order by patid, fst_dt;

-- COMMAND ----------

select loinc_cd, count(distinct patid) as cnts from ac_dod_t1d_antibody_lab_4
where Flag_Result='Positive'
group by 1;

-- COMMAND ----------

-- MAGIC %md ##### Working on one LOINC 13927-9 (Because it's a different scenario)

-- COMMAND ----------

create or replace table ac_dod_t1d_antibdy_lab_loinc_13927 as 
select distinct *,
case when loinc_cd='13927-9' and rslt_txt = '1:02' then 2*5
when loinc_cd='13927-9' and rslt_txt = '1:16' then 16*5
when loinc_cd='13927-9' and rslt_txt = '1:08' then 8*5
when loinc_cd='13927-9' and rslt_txt = '1:32' then 32*5
when loinc_cd='13927-9' and rslt_txt = '1:04' then 4*5
else result_value end as Loinc_value

from ac_dod_t1d_antibody_lab_3
where loinc_cd='13927-9';

select distinct * from ac_dod_t1d_antibdy_lab_loinc_13927
order by patid, fst_dt;




-- COMMAND ----------

select count(distinct patid) as cnts from ac_dod_t1d_antibdy_lab_loinc_13927
where loinc_value>=10
