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

-- MAGIC %md #### Checking all lab data

-- COMMAND ----------

select distinct test_name from ac_labs_ehr_202211
where lower(test_name) like '%antibod%';

-- COMMAND ----------

create or replace  table ac_ehr_islet_lab_202211_1 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date
from ac_labs_ehr_202211 a 
where 
test_name in ('Insulin antibody','Islet antigen 2 antibody (IA-2A)','Glutamic acid decarboxylase-65 antibody (GAD65)','Zinc transporter 8 antibody (ZnT8)','Pancreatic islet cell antibody (ICA)') ;

-- COMMAND ----------

select distinct * from ac_ehr_islet_lab_202211_1
order by ptid, service_date;

select count(distinct ptid) from ac_ehr_islet_lab_202211_1; -- 147754

-- COMMAND ----------

-- MAGIC %md #### Checking procedure table for islet 

-- COMMAND ----------

create or replace table ac_ehr_islet_proc_202211_1 as
select distinct ptid, encid, proc_date, proc_code, proc_desc, proc_code_type from ac_Proc_ehr_202211
where proc_code in ('86341','86337');

select distinct * from ac_ehr_islet_proc_202211_1
order by ptid, proc_date

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_islet_proc_202211_1; -- 83087

-- COMMAND ----------

-- MAGIC %md #### Combining lab and procedure islet tables

-- COMMAND ----------

create or replace table ac_ehr_islet_proc_lab_1 as
select distinct ptid, encid, service_date from ac_ehr_islet_lab_202211_1
union
select distinct ptid, encid, proc_date from ac_ehr_islet_proc_202211_1;

select distinct * from ac_ehr_islet_proc_lab_1;

-- COMMAND ----------

create or replace table ac_ehr_islet_proc_lab_2 as
select distinct ptid, count(distinct service_date) as n_islet from ac_ehr_islet_proc_lab_1
group by 1
order by 1;

select distinct * from ac_ehr_islet_proc_lab_2
where ptid='PT083305590';

-- COMMAND ----------

select distinct * from ac_ehr_islet_proc_lab_1
where ptid='PT083305590';

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_islet_proc_lab_2;
select count(distinct ptid) from ac_ehr_islet_proc_lab_2
where n_islet>=2;

select count(distinct a.ptid) from ac_ehr_islet_proc_lab_2 a
inner join ac_ehr_dx_t1d_full b on a.ptid=b.ptid;
select count(distinct a.ptid) from ac_ehr_islet_proc_lab_2 a
inner join ac_ehr_dx_t1d_full b on a.ptid=b.ptid
where n_islet>=2;


-- COMMAND ----------

-- MAGIC %md #### Checking lab table for dysglycemia

-- COMMAND ----------

select distinct test_name from ac_labs_ehr_202211
where lower(test_name) like '%dysglycemia%' or lower(test_name) like '%glucose%' or lower(test_name) like '%hemoglobin%' or
lower(test_name) like '%glycosylated%';

-- COMMAND ----------

create or replace  table ac_ehr_dysgly_lab_202211_1 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date
from ac_labs_ehr_202211 a 
where 
test_name in ('Glucose.tolerance test.3 hour','Glucose.fasting','Hemoglobin A1C','Glucose.tolerance test.1 hour','Glucose.tolerance test.2 hour') ;

select distinct * from ac_ehr_dysgly_lab_202211_1
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_dysgly_proc_202211_1 as
select distinct ptid, encid, proc_date, proc_code, proc_desc, proc_code_type from ac_Proc_ehr_202211
where proc_code in ('82951', '82947', '82950', '83036') ;

select distinct * from ac_ehr_dysgly_proc_202211_1
order by ptid, proc_date

-- COMMAND ----------

-- MAGIC %md #### Combining lab and proc tables

-- COMMAND ----------

create or replace table ac_ehr_dysgly_proc_lab_1 as
select distinct ptid, encid, service_date from ac_ehr_dysgly_lab_202211_1
union
select distinct ptid, encid, proc_date from ac_ehr_dysgly_proc_202211_1;

select distinct * from ac_ehr_dysgly_proc_lab_1;

-- COMMAND ----------

create or replace table ac_ehr_dysgly_proc_lab_2 as
select distinct ptid, count(distinct service_date) as n_dysgly from ac_ehr_dysgly_proc_lab_1
group by 1
order by 1;

select distinct * from ac_ehr_dysgly_proc_lab_2
-- where ptid='PT083305590';

-- COMMAND ----------

select distinct ptid, service_date from ac_ehr_dysgly_proc_lab_1
where ptid='PT345505033'
order by ptid, service_date;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dysgly_proc_lab_2;

select count(distinct a.ptid) from ac_ehr_islet_proc_lab_2 a
inner join ac_ehr_dysgly_proc_lab_2 b on a.ptid=b.ptid
where a.n_islet>=2;


select count(distinct a.ptid) from ac_ehr_dysgly_proc_lab_2 a
inner join ac_ehr_dx_t1d_full b on a.ptid=b.ptid;

select count(distinct a.ptid) from ac_ehr_islet_proc_lab_2 a
inner join ac_ehr_dysgly_proc_lab_2 b on a.ptid=b.ptid
inner join ac_ehr_dx_t1d_full c on a.ptid=c.ptid
where a.n_islet>=2;



-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_t1d_full
