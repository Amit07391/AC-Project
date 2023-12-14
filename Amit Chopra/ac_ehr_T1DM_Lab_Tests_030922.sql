-- Databricks notebook source
-- MAGIC %md #### T1D Diagnosis in the data

-- COMMAND ----------

drop table if exists ac_ehr_dx_t1d_full;
create or replace table ac_ehr_dx_t1d_full as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE,
b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_ehr_diag_202211 a join ty00_all_dx_comorb b
on a.DIAGNOSIS_CD=b.code
where
 b.dx_name in ('T1DM')
and DIAGNOSIS_STATUS='Diagnosis of';

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_t1d_full; -- 733186

-- COMMAND ----------

select max(coalesce(result_date,collected_date,order_date)) as dt from ac_labs_ehr_202211

-- COMMAND ----------

-- MAGIC %md #### Checking all lab data

-- COMMAND ----------

select distinct test_name from ac_labs_ehr_202211
where lower(test_name) like '%antibod%';

-- COMMAND ----------

create or replace  table ac_ehr_T1D_lab_tests_1 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date
from ac_labs_ehr_202211 a 
where 
test_name in ('Islet antigen 2 antibody (IA-2A)','Zinc transporter 8 antibody (ZnT8)','Pancreatic islet cell antibody (ICA)',
'Glucose.tolerance test.3 hour','Glucose.fasting','Glucose.tolerance test.1 hour','Glucose.tolerance test.2 hour','Insulin antibody')
and coalesce(a.result_date,a.collected_date,a.order_date) between '2021-01-01' and '2022-12-31';

-- COMMAND ----------

select distinct * from ac_ehr_T1D_lab_tests_1
order by ptid, service_date;

select test_name, count(distinct ptid) from ac_ehr_T1D_lab_tests_1
group by 1; -- 147754

-- COMMAND ----------

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Glucose.tolerance test.3 hour';

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Glucose.tolerance test.2 hour';

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Glucose.tolerance test.1 hour';

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Glucose.fasting';

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Islet antigen 2 antibody (IA-2A)';

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Zinc transporter 8 antibody (ZnT8)';

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Pancreatic islet cell antibody (ICA)';

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_1
where test_name='Insulin antibody';

-- COMMAND ----------

-- MAGIC %md #### Adding demographics

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_demo as
select a.*, b.birth_yr, b.gender, b.race, b.ethnicity, b.region, b.division from ac_ehr_T1D_lab_tests_1 a
left join 
ac_Patient_ehr_202211 b on a.ptid=b.ptid;

select distinct * from ac_ehr_T1D_lab_tests_demo;

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_index as
select distinct ptid, min(service_date) as indx_dt from ac_ehr_T1D_lab_tests_demo
group by 1;

select distinct * from ac_ehr_T1D_lab_tests_index;

select count(distinct ptid) from ac_ehr_T1D_lab_tests_index;  --403458

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_demo_1 as
select distinct a.*, b.indx_dt, year(b.indx_dt) as yr_indx,
case when race='Caucasian' and ethnicity in ('Not Hispanic','Unknown') then 'Caucasian'
when race='African American' and ethnicity in ('Not Hispanic','Unknown') then 'African American'
when race='Asian' and ethnicity in ('Not Hispanic','Unknown') then 'Asian'
when race in ('Asian','Caucasian','African American','Other/Unknown') and ethnicity ='Hispanic' then 'Hispanic'
else 'Other/Unknown' end as Race_new from ac_ehr_T1D_lab_tests_demo a
inner join ac_ehr_T1D_lab_tests_index b on a.ptid=b.ptid;

select distinct race, ethnicity, race_new from ac_ehr_T1D_lab_tests_demo_1;

-- COMMAND ----------

select test_name, gender, count(distinct ptid) from ac_ehr_T1D_lab_tests_demo_1
group by 1,2
order by 1,2; 

select test_name, race_new, count(distinct ptid) from ac_ehr_T1D_lab_tests_demo_1
group by 1,2
order by 1,2; 

select test_name, region, count(distinct ptid) from ac_ehr_T1D_lab_tests_demo_1
group by 1,2
order by 1,2; 

select test_name, division, count(distinct ptid) from ac_ehr_T1D_lab_tests_demo_1
group by 1,2
order by 1,2; 



-- COMMAND ----------

-- MAGIC %md #### Checking positive negative results for dysglycemia

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_dys_1 as
select distinct *, cast(test_result as float) as test_rslt from ac_ehr_T1D_lab_tests_1
where test_name in ('Glucose.tolerance test.3 hour','Glucose.fasting','Glucose.tolerance test.1 hour','Glucose.tolerance test.2 hour')
order by ptid, service_date;

select distinct * from ac_ehr_T1D_lab_tests_dys_1
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_dys_2 as
select distinct *, case when test_rslt between 140 and 199 and test_name <>'Glucose.fasting' then 'Positive'
when test_rslt between 100 and 125 and test_name = 'Glucose.fasting' then 'Positive'
else 'Negative' end as Result_flag
from ac_ehr_T1D_lab_tests_dys_1;

select distinct * from ac_ehr_T1D_lab_tests_dys_2
order by ptid, service_date;

-- COMMAND ----------

select test_Name, count(distinct ptid) from ac_ehr_T1D_lab_tests_dys_2
where Result_flag='Positive'
group by 1;

-- COMMAND ----------

-- MAGIC %md #### Checking positive negative for antibody tests

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_antibdy_1 as
select distinct *, cast(test_result as float) as test_rslt from ac_ehr_T1D_lab_tests_1
where test_name not in ('Glucose.tolerance test.3 hour','Glucose.fasting','Glucose.tolerance test.1 hour','Glucose.tolerance test.2 hour')
order by ptid, service_date;

select distinct * from ac_ehr_T1D_lab_tests_antibdy_1
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_antibdy_2 as
select distinct *,
case when result_unit in ('nmol/l','nanomole/liter','nmole/liter') then (test_rslt*1000)
else test_rslt end as test_rslt_1
from ac_ehr_T1D_lab_tests_antibdy_1;

select distinct * from ac_ehr_T1D_lab_tests_antibdy_2
order by ptid, service_date;

-- COMMAND ----------


create or replace table ac_ehr_T1D_lab_tests_antibdy_3 as
select distinct *,
case when result_unit in ('nmol/l','nanomole/liter','nmole/liter') then test_rslt_1/6
else test_rslt_1 end as test_rslt_new
from ac_ehr_T1D_lab_tests_antibdy_2;


select distinct * from ac_ehr_T1D_lab_tests_antibdy_3
order by ptid, service_date;

-- COMMAND ----------

select distinct * from ac_ehr_T1D_lab_tests_antibdy_3
where result_unit in ('nmol/l','nanomole/liter','nmole/liter') 

-- COMMAND ----------

-- MAGIC %md #### Checking only Zinc and Insulin antibody

-- COMMAND ----------

-- drop table ac_ehr_T1D_lab_tests_antibdy_3A;
create or replace table ac_ehr_T1D_lab_tests_antibdy_3A as
select distinct *,
case when test_name='Insulin antibody' and test_rslt_new>=0.4 then 'Positive'
when test_name='Zinc transporter 8 antibody (ZnT8)' and test_rslt_new>=15 then 'Positive'
else 'Negative' end as Flag
from ac_ehr_T1D_lab_tests_antibdy_3
where test_name in ('Insulin antibody','Zinc transporter 8 antibody (ZnT8)');


select distinct * from ac_ehr_T1D_lab_tests_antibdy_3A
order by ptid, service_date;

-- COMMAND ----------

select test_name, count(distinct ptid) from ac_ehr_T1D_lab_tests_antibdy_3A
where flag='Positive'
group by 1

-- COMMAND ----------

-- MAGIC %md #### Checking for islet cell and pancreatic islet cell test

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_antibdy_4A as
select distinct *,
case when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='1:08' then 8*5
when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='1:8' then 8*5
when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='1:32' then 32*5
when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='1:16' then 16*5
when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='1:64' then 64*5
when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='1:128' then 128*5
when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='>1:1024' then 1024*5
when test_name='Islet antigen 2 antibody (IA-2A)' and test_result='1:512' then 512*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:08' then 8*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:8' then 8*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:32' then 32*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:256' then 256*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:512' then 512*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:04' then 4*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:4' then 4*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:16' then 16*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='>1:1024' then 1024*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:1024' then 1024*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:64' then 64*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:128' then 128*5
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='1:16' then 16*5
else 0 end as Test_result_JDF,
case when result_unit='ku/l' then test_rslt_new*1000 
else test_rslt_new end as test_rslt_new_2

from ac_ehr_T1D_lab_tests_antibdy_3
where test_name in ('Pancreatic islet cell antibody (ICA)','Islet antigen 2 antibody (IA-2A)');

select distinct * from ac_ehr_T1D_lab_tests_antibdy_4A
order by ptid, service_date;

-- COMMAND ----------

select distinct test_result, result_unit from ac_ehr_T1D_lab_tests_antibdy_4A
where test_name='Pancreatic islet cell antibody (ICA)';

-- COMMAND ----------

select distinct * from ac_ehr_T1D_lab_tests_antibdy_4A
where test_name='Pancreatic islet cell antibody (ICA)';-- where result_unit='ku/l'

-- COMMAND ----------

create or replace table ac_ehr_T1D_lab_tests_antibdy_4B as
select distinct *,
case when test_name='Islet antigen 2 antibody (IA-2A)' and test_rslt_new_2>=5.4 then 'Positive'
when test_name='Islet antigen 2 antibody (IA-2A)' and Test_result_JDF>=10 then 'Positive'
when test_name='Pancreatic islet cell antibody (ICA)' and Test_result_JDF>=10 then 'Positive'
when test_name='Pancreatic islet cell antibody (ICA)' and result_unit='jdf units' and test_rslt_new_2>=10 then 'Positive'
when test_name='Pancreatic islet cell antibody (ICA)' and test_result='positive' then 'Positive'
when test_name='Pancreatic islet cell antibody (ICA)' and test_rslt_new_2>1.05 and result_unit<>'jdf units' then 'Positive' 
else 'Negative' end as test_flag
from ac_ehr_T1D_lab_tests_antibdy_4A;

select distinct * from ac_ehr_T1D_lab_tests_antibdy_4B
order by ptid, service_date;

-- COMMAND ----------

select distinct * from ac_ehr_T1D_lab_tests_antibdy_4B
where result_unit like '%jdf%'

-- COMMAND ----------

select test_name, count(distinct ptid) from ac_ehr_T1D_lab_tests_antibdy_4B
where test_flag="Positive"
group by 1;
