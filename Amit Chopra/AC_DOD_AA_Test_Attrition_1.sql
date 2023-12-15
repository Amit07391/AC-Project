-- Databricks notebook source


-- drop table if exists ac_dod_2305_mem_cont_enrol;
-- create table ac_dod_2305_mem_cont_enrol using delta location 'dbfs:/mnt/optumclin/202305/ontology/base/dod/Member Continuous Enrollment';

-- select distinct * from ac_dod_2305_mem_cont_enrol;

drop table if exists ac_dod_2305_lab_results;
create table ac_dod_2305_lab_results using delta location 'dbfs:/mnt/optumclin/202305/ontology/base/dod/Lab Results';

select distinct * from ac_dod_2305_lab_results;

-- COMMAND ----------

create or replace table ac_dod_AA_lab_202305_1 as 
select distinct *, cast(rslt_txt as double) as result from ac_dod_2305_lab_results
where loinc_cd in ('56540-8',
'13926-1',
'72523-4',
'30347-9',
'94345-6',
'53708-4',
'58451-6',
'90829-3',
'94359-7',
'42501-7',
'31209-0',
'56718-0',
'60463-7',
'81155-4',
'32636-3',
'13927-9',
'5265-4',
'8086-1',
'45171-6',
'56687-7',
'31547-3',
'5232-4',
'8072-1',
'2481-0',
'56546-5',
'60463-7',
'11087-4',
'2482-8',
'76651-9')
order by patid, fst_dt;

select distinct * from ac_dod_AA_lab_202305_1
order by patid, fst_dt;

-- COMMAND ----------

select distinct tst_desc from ac_dod_AA_lab_202305_1;

-- COMMAND ----------

select distinct * from sg_autoantibody_codes

-- COMMAND ----------

create or replace table ac_dod_AA_lab_16_22_1 as
select distinct a.*, b.AA_TEST from ac_dod_AA_lab_202305_1 a
inner join sg_autoantibody_codes b on a.LOINC_CD=b.LOINC_CD
where FST_DT >='2016-01-01' and FST_DT<='2022-12-31'
order by patid, FST_DT;

select distinct * from ac_dod_AA_lab_16_22_1
order by patid, FST_DT;

-- COMMAND ----------

select count(distinct patid) from ac_dod_AA_lab_16_22_1

-- COMMAND ----------

create or replace table ac_dod_AA_lab_16_22_indx as
select distinct patid, min(fst_dt) as index_date from ac_dod_AA_lab_16_22_1
group by 1
order by 1;

select distinct * from ac_dod_AA_lab_16_22_indx
order by 1;

-- COMMAND ----------

create or replace table ac_dod_AA_test_eligible as
select distinct a.patid, a.index_date from ac_dod_AA_lab_16_22_indx a
left join ac_dod_2305_mem_cont_enrol b on a.patid=b.PATID
where b.ELIGEFF<=a.index_date - 180 and b.ELIGEND>=a.index_date +30;



-- COMMAND ----------


select count(distinct patid) from ac_dod_AA_test_eligible

-- COMMAND ----------

select distinct * from sg_autoantibody_codes

-- COMMAND ----------

create or replace table ac_dod_AA_tests_15_23 as
select distinct a.*, b.index_date, d.AA_TEST from ac_dod_AA_lab_202305_1 a
inner join ac_dod_AA_test_eligible b on a.PATID=b.patid
inner join sg_autoantibody_codes d on a.LOINC_CD=d.LOINC_CD
where a.FST_DT >='2015-07-01' and a.FST_DT<='2023-01-31';

select distinct * from ac_dod_AA_tests_15_23
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from ac_dod_AA_tests_15_23
where patid='33003288597'

-- COMMAND ----------

create or replace table ac_dod_AA_tests_cnt as
select distinct * from (select distinct patid, count(distinct AA_test) as test_cnt from ac_dod_AA_tests_15_23
group by 1
order by 1)
where test_cnt>=2;

select distinct * from ac_dod_AA_tests_cnt
order by 1;

-- COMMAND ----------

select count(distinct patid) from ac_dod_AA_tests_cnt

-- COMMAND ----------

create or replace table ac_dod_AA_16_22_All_test as
select distinct a.* from ac_dod_AA_lab_16_22_1 a
inner join ac_dod_AA_tests_cnt b on a.PATID=b.patid
order by a.PATID, a.FST_DT;

select distinct * from ac_dod_AA_16_22_All_test
order by patid, fst_dt;


-- COMMAND ----------

select aa_test, count(distinct patid) from ac_dod_AA_16_22_All_test
group by 1
order by 1

-- COMMAND ----------

create or replace table ac_dod_AA_tests_value as
select distinct a.*, 
case when rslt_nbr>0 then rslt_nbr
when rslt_nbr=0 and isnotnull(result) then result
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%' or rslt_txt like '%=%' or substr(rslt_txt,-1)='%') then cast(rslt_txt as double)
when rslt_nbr=0 and isnull(result) and rslt_txt like '>%' and not(rslt_txt like '>=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '>=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<%' and not(rslt_txt like '<=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)-0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)-0.1
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%') and substr(rslt_txt,-1)='%' then cast(substring_index(rslt_txt,'%',1) as double)
else null end value 
from ac_dod_AA_tests_15_23 a
inner join ac_dod_AA_tests_cnt b on a.patid=b.patid
order by patid, fst_dt;

select distinct * from ac_dod_AA_tests_value
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_AA_tests_value

-- COMMAND ----------

create or replace table ac_dod_lab_antibody_select as
select distinct *
from ac_dod_AA_tests_value
where (AA_TEST in ('IA-2A') and value>=5.4)
   or (AA_TEST in ('IAA') and value>=0.4)
   or (AA_TEST in ('GAD') and value>=5.0)
   or (AA_TEST in ('ICA') and lcase(rslt_txt) in ('1:128','1:16','1:256','1:32','1:4','1:64','1:8','positive'))
   or (AA_TEST in ('ZnT8A') and value>=15)
order by patid, fst_dt;

select distinct * from ac_dod_lab_antibody_select
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_AA_pos_tests_cnt as
select distinct * from (select distinct patid, count(distinct AA_test) as test_cnt from ac_dod_lab_antibody_select
group by 1
order by 1)
where test_cnt>=2;

select distinct * from ac_dod_AA_pos_tests_cnt
order by 1;

-- COMMAND ----------

select distinct * from ac_dod_lab_antibody_select
where patid='33003490453'

-- COMMAND ----------

select count(distinct patid) from ac_dod_AA_pos_tests_cnt

-- COMMAND ----------

select year(b.index_date) as yr_index, count(distinct a.patid) as pts from ac_dod_AA_tests_cnt a
left join ac_dod_AA_tests_15_23 b on a.patid=b.patid
group by 1
order by 1;

-- COMMAND ----------

select count(distinct patid) from ac_dod_AA_tests_cnt
where test_cnt=4

-- COMMAND ----------



create table ac_dod_AA_16_22_All_test_value as
select distinct *, case when rslt_nbr>0 then rslt_nbr
when rslt_nbr=0 and isnotnull(result) then result
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%' or rslt_txt like '%=%' or substr(rslt_txt,-1)='%') then cast(rslt_txt as double)
when rslt_nbr=0 and isnull(result) and rslt_txt like '>%' and not(rslt_txt like '>=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '>=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<%' and not(rslt_txt like '<=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)-0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)-0.1
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%') and substr(rslt_txt,-1)='%' then cast(substring_index(rslt_txt,'%',1) as double)
else null end value  from ac_dod_AA_16_22_All_test
order by patid, fst_dt;

select distinct * from ac_dod_AA_16_22_All_test_value
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from ac_dod_AA_16_22_All_test_value
where AA_test='ZnT8A' and value>=15

-- COMMAND ----------

create or replace table ac_dod_AA_16_22_All_test_posit_final as
select distinct * from ac_dod_AA_16_22_All_test_value
where (AA_TEST in ('IA-2A') and value>=5.4)
   or (AA_TEST in ('IAA') and value>=0.4)
   or (AA_TEST in ('GAD') and value>=5.0)
   or (AA_TEST in ('ICA') and lcase(rslt_txt) in ('1:128','1:16','1:256','1:32','1:4','1:64','1:8','positive'))
   or (AA_TEST in ('ZnT8A') and value>=15)
order by patid, fst_dt;

select distinct * from ac_dod_AA_16_22_All_test_posit_final
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from ac_dod_AA_16_22_All_test_posit_final
where AA_test='ZnT8A'

-- COMMAND ----------

select AA_test, count(distinct patid) from ac_dod_AA_16_22_All_test_posit_final
group by 1
order by 1

-- COMMAND ----------

select year(b.index_date) as yr_index, count(distinct a.patid) as pts from ac_dod_AA_tests_cnt a
left join ac_dod_AA_tests_15_23 b on a.patid=b.patid
group by 1
order by 1;

-- COMMAND ----------

create table ac_lab_a1c_loinc as
select distinct *, cast(rslt_txt as double) as result
from ac_dod_2305_lab_results
where lcase(loinc_cd) in ('17855-8', '17856-6','41995-2','4548-4','45484','4637-5','55454-3','hgba1c') or (isnull(loinc_cd) and lcase(tst_desc) in ('a1c','glyco hemoglobin a1c','glycohemoglobin (a1c) glycohem','glycohemoglobin a1c','hemoglobin a1c','hgb a1c','hba1c','hemoglob a1c','hemoglobin a1c'
,'hemoglobin a1c w/o eag','hgb-a1c','hgba1c-t'))
order by patid, fst_dt;

select distinct * from ac_lab_a1c_loinc
order by patid, fst_dt;

-- COMMAND ----------



create table ac_lab_a1c_loinc_value as
select distinct *, case when rslt_nbr>0 then rslt_nbr
when rslt_nbr=0 and isnotnull(result) then result
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%' or rslt_txt like '%=%' or substr(rslt_txt,-1)='%') then cast(rslt_txt as double)
when rslt_nbr=0 and isnull(result) and rslt_txt like '>%' and not(rslt_txt like '>=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '>=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)+0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<%' and not(rslt_txt like '<=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)-0.1
when rslt_nbr=0 and isnull(result) and rslt_txt like '<=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)-0.1
when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%') and substr(rslt_txt,-1)='%' then cast(substring_index(rslt_txt,'%',1) as double)
else null end value  from ac_lab_a1c_loinc
order by patid, fst_dt;

select distinct * from ac_lab_a1c_loinc_value
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_AA_test_a1c_bl as
select distinct a.*, c.index_date from ac_lab_a1c_loinc_value a
inner join ac_dod_AA_tests_cnt b on a.PATID=b.patid
left join ac_dod_AA_tests_15_23 c on a.patid=c.patid
where a.FST_DT>= c.index_date - 180 and a.FST_DT<=c.index_date - 1
order by a.PATID, a.FST_DT;

select distinct * from ac_dod_AA_test_a1c_bl
order by patid, fst_dt;

-- COMMAND ----------

select count(distinct patid) from ac_dod_AA_test_a1c_bl
where value>=6.5
