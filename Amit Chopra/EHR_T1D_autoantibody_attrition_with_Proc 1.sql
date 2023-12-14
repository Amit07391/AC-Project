-- Databricks notebook source
-- drop table if exists ac_ehr_lab_202305;
-- CREATE TABLE ac_ehr_lab_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Lab";

-- select distinct * from ac_ehr_lab_202305;

-- drop table if exists ac_ehr_patient_202305;
-- CREATE TABLE ac_ehr_patient_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Patient";

-- select distinct * from ac_ehr_patient_202305;


-- drop table if exists ac_ehr_insurance_202305;
-- CREATE TABLE ac_ehr_insurance_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Insurance";

-- select distinct * from ac_ehr_insurance_202305;

-- drop table if exists ac_ehr_nlp_biomark_202305;
-- CREATE TABLE ac_ehr_nlp_biomark_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/NLP Biomarker";

-- select distinct * from ac_ehr_nlp_biomark_202305;


-- drop table if exists ac_ehr_obs_202305;
-- CREATE TABLE ac_ehr_obs_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Observation";

-- select distinct * from ac_ehr_obs_202305;


-- drop table if exists ac_ehr_prov_202305;
-- CREATE TABLE ac_ehr_prov_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Provider";

-- select distinct * from ac_ehr_prov_202305;

-- drop table if exists ac_ehr_enc_prov_202305;
-- CREATE TABLE ac_ehr_enc_prov_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Encounter Provider";

-- select distinct * from ac_ehr_enc_prov_202305;


-- drop table if exists ac_ehr_enc_202305;
-- CREATE TABLE ac_ehr_enc_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Encounter";

-- select distinct * from ac_ehr_enc_202305;

drop table if exists ac_ehr_proc_202305;
CREATE TABLE ac_ehr_proc_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/Procedure";

select distinct * from ac_ehr_proc_202305;







-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_t1d_full; -- 733186

-- COMMAND ----------

-- MAGIC %md #### Checking all lab data

-- COMMAND ----------

select distinct test_name from ac_labs_ehr_202211
where lower(test_name) like '%antibod%';

-- COMMAND ----------

create or replace  table ac_ehr_islet_lab_202305_1 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date
from ac_ehr_lab_202305 a 
where 
test_name in ('Insulin antibody','Islet antigen 2 antibody (IA-2A)','Glutamic acid decarboxylase-65 antibody (GAD65)','Zinc transporter 8 antibody (ZnT8)','Pancreatic islet cell antibody (ICA)') ;

select distinct * from ac_ehr_islet_lab_202305_1
order by ptid, service_date;

-- COMMAND ----------



select count(distinct ptid) from ac_ehr_islet_lab_202305_1; -- 147754

-- COMMAND ----------

create or replace table ac_ehr_islet_lab_16_22_1 as
select distinct * from ac_ehr_islet_lab_202305_1
where service_date >='2016-01-01' and service_date<='2022-12-31'
order by ptid, service_date;

select distinct * from ac_ehr_islet_lab_16_22_1
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_islet_proc_202305_1 as
select distinct ptid, encid, proc_date, proc_code, proc_desc from ac_ehr_proc_202305
where proc_code in ('86341','86337')
order by ptid, proc_date;

select distinct * from ac_ehr_islet_proc_202305_1
order by ptid, proc_date;

-- COMMAND ----------

create or replace table ac_ehr_islet_proc_16_22_1 as
select distinct * from ac_ehr_islet_proc_202305_1
where proc_date >='2016-01-01' and proc_date<='2022-12-31' and
proc_desc in ('ISLET CELL ANTIBODY IGG',
'INSULIN ANTIBODIES',
'INSULIN ANTIBODIES ; SERUM',
'INSULIN ANTIBIOTICS',
'HEMOGLOBIN INSULIN ANTIBODY',
'GENERALIZED ANXIETY DISORDER 65 ; IA - 2 ; AND INSULIN AUTOANTIBODY',
'HC INSULIN ANTIBODIES',
'INSULIN ANTIBODIES MAYO M 8666',
'HC ISLET CELL ANTIBODY',
'HEMOGLOBIN ISLET CELL ANTIBODY ; IGG',
'HC INSULIN ABS',
'IA - 2 ANTIBODY',
'HC GENERALIZED ANXIETY DISORDER - 65 AB',
'ISLET CELL AB',
'GENERALIZED ANXIETY DISORDER 65',
'IA - 2 AUTOANTIBODIES',
'INSULIN ANTIBODIES - S',
'LB - INSULIN ANTIBODIES',
'HEMOGLOBIN INSULIN AUTOANTIBODIES',
'HEMOGLOBIN INSULIN ANTIBODIES',
'ANTI- INSULIN ANTIBODIES - R/L',
'ANTI-ISLET CELL ANTIBODY',
'HC ISLET CELL AB',
'HC GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'GLUTAMIC ACID DECARBOXYLASE',
'INSULIN ; ANTIBODIES',
'HC INSULIN AB',
'ISLET CELL ANTIBODY ; IGG',
'HC INSULIN AUTOANTIBODIES',
'HEMOGLOBIN IA - 2 ANTIBODY',
'IA - 2 AB',
'CHG GLUTAMIC ACID DECARBOXYLASE AB GAD65',
'GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'HEMOGLOBIN ISLET CELL ANTIBODY',
'INSULIN AUTOANTIBODIES - 141598',
'GENERALIZED ANXIETY DISORDER - 65 AUTOANTIBODY',
'INSULIN AUTOANTIBODIES',
'INSULIN ANTIBODY',
'INSULIN AB',
'INSULIN ANTIBODIES SERUM',
'ISLET CELL ANTIBODY',
'INSULIN AUTOANTIBODY',
'IA2 AUTOANTIBODIES - 141531',
'GENERALIZED ANXIETY DISORDER 65 S')
order by ptid, proc_date;

select distinct * from ac_ehr_islet_proc_16_22_1
order by ptid, proc_date;

-- COMMAND ----------

select distinct proc_desc from ac_ehr_islet_proc_16_22_1;
select distinct test_name from ac_ehr_islet_lab_16_22_1;


-- COMMAND ----------

create or replace table ac_ehr_islet_proc_16_22_2 as
select distinct *,
case when proc_desc in ('GENERALIZED ANXIETY DISORDER 65 ; IA - 2 ; AND INSULIN AUTOANTIBODY',
'HC GENERALIZED ANXIETY DISORDER - 65 AB',
'GENERALIZED ANXIETY DISORDER 65',
'HC GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'GLUTAMIC ACID DECARBOXYLASE',
'CHG GLUTAMIC ACID DECARBOXYLASE AB GAD65',
'GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'GENERALIZED ANXIETY DISORDER - 65 AUTOANTIBODY',
'GENERALIZED ANXIETY DISORDER 65 S') then 'Glutamic acid decarboxylase-65 antibody (GAD65)'
when proc_desc in ('INSULIN ANTIBODIES',
'INSULIN ANTIBODIES ; SERUM',
'INSULIN ANTIBIOTICS',
'HEMOGLOBIN INSULIN ANTIBODY',
'HC INSULIN ANTIBODIES',
'INSULIN ANTIBODIES MAYO M 8666',
'HC INSULIN ABS',
'INSULIN ANTIBODIES - S',
'LB - INSULIN ANTIBODIES',
'HEMOGLOBIN INSULIN AUTOANTIBODIES',
'HEMOGLOBIN INSULIN ANTIBODIES',
'ANTI- INSULIN ANTIBODIES - R/L',
'INSULIN ; ANTIBODIES',
'HC INSULIN AB',
'HC INSULIN AUTOANTIBODIES',
'INSULIN AUTOANTIBODIES - 141598',
'INSULIN AUTOANTIBODIES',
'INSULIN ANTIBODY',
'INSULIN AB',
'INSULIN ANTIBODIES SERUM',
'INSULIN AUTOANTIBODY') then 'Insulin antibody'
when proc_desc in ('IA - 2 ANTIBODY',
'IA - 2 AUTOANTIBODIES',
'HEMOGLOBIN IA - 2 ANTIBODY',
'IA - 2 AB',
'IA2 AUTOANTIBODIES - 141531') then 'Islet antigen 2 antibody (IA-2A)'
when proc_desc in ('HC ISLET CELL ANTIBODY','ISLET CELL ANTIBODY IGG',
'HEMOGLOBIN ISLET CELL ANTIBODY ; IGG',
'ISLET CELL AB',
'ANTI-ISLET CELL ANTIBODY',
'HC ISLET CELL AB',
'ISLET CELL ANTIBODY ; IGG',
'HEMOGLOBIN ISLET CELL ANTIBODY','ISLET CELL ANTIBODY') then 'Pancreatic islet cell antibody (ICA)'
else 'Others' end as Test_flag
 from ac_ehr_islet_proc_16_22_1
where proc_desc in ('ISLET CELL ANTIBODY IGG',
'INSULIN ANTIBODIES',
'INSULIN ANTIBODIES ; SERUM',
'INSULIN ANTIBIOTICS',
'HEMOGLOBIN INSULIN ANTIBODY',
'GENERALIZED ANXIETY DISORDER 65 ; IA - 2 ; AND INSULIN AUTOANTIBODY',
'HC INSULIN ANTIBODIES',
'INSULIN ANTIBODIES MAYO M 8666',
'HC ISLET CELL ANTIBODY',
'HEMOGLOBIN ISLET CELL ANTIBODY ; IGG',
'HC INSULIN ABS',
'IA - 2 ANTIBODY',
'HC GENERALIZED ANXIETY DISORDER - 65 AB',
'ISLET CELL AB',
'GENERALIZED ANXIETY DISORDER 65',
'IA - 2 AUTOANTIBODIES',
'INSULIN ANTIBODIES - S',
'LB - INSULIN ANTIBODIES',
'HEMOGLOBIN INSULIN AUTOANTIBODIES',
'HEMOGLOBIN INSULIN ANTIBODIES',
'ANTI- INSULIN ANTIBODIES - R/L',
'ANTI-ISLET CELL ANTIBODY',
'HC ISLET CELL AB',
'HC GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'GLUTAMIC ACID DECARBOXYLASE',
'INSULIN ; ANTIBODIES',
'HC INSULIN AB',
'ISLET CELL ANTIBODY ; IGG',
'HC INSULIN AUTOANTIBODIES',
'HEMOGLOBIN IA - 2 ANTIBODY',
'IA - 2 AB',
'CHG GLUTAMIC ACID DECARBOXYLASE AB GAD65',
'GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'HEMOGLOBIN ISLET CELL ANTIBODY',
'INSULIN AUTOANTIBODIES - 141598',
'GENERALIZED ANXIETY DISORDER - 65 AUTOANTIBODY',
'INSULIN AUTOANTIBODIES',
'INSULIN ANTIBODY',
'INSULIN AB',
'INSULIN ANTIBODIES SERUM',
'ISLET CELL ANTIBODY',
'INSULIN AUTOANTIBODY',
'IA2 AUTOANTIBODIES - 141531',
'GENERALIZED ANXIETY DISORDER 65 S')
order by ptid, proc_date;

select distinct * from ac_ehr_islet_proc_16_22_2 
order by ptid, proc_date;


-- COMMAND ----------

create or replace table ac_ehr_AA_test_lab_proc as
select distinct ptid, service_date, test_name, 'lab' as source from ac_ehr_islet_lab_16_22_1
union
select distinct ptid, proc_date, test_flag, 'proc' as source from ac_ehr_islet_proc_16_22_2;

select distinct * from ac_ehr_AA_test_lab_proc
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_islet_lab_proc_16_22_indx as
select distinct ptid, min(service_date) as index_date from ac_ehr_AA_test_lab_proc
group by 1
order by 1;

select distinct * from ac_ehr_islet_lab_proc_16_22_indx
order by 1;


-- COMMAND ----------

select distinct test_name from ac_ehr_islet_lab_16_22_1

-- COMMAND ----------

create table ac_ehr_AA_tests_lab_proc_full as
select distinct ptid, service_date, test_name from ac_ehr_islet_lab_202305_1
where service_date >='2015-07-01' and service_date<='2023-01-31'
union
select distinct ptid, proc_date, case when proc_desc in ('GENERALIZED ANXIETY DISORDER 65 ; IA - 2 ; AND INSULIN AUTOANTIBODY',
'HC GENERALIZED ANXIETY DISORDER - 65 AB',
'GENERALIZED ANXIETY DISORDER 65',
'HC GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'GLUTAMIC ACID DECARBOXYLASE',
'CHG GLUTAMIC ACID DECARBOXYLASE AB GAD65',
'GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'GENERALIZED ANXIETY DISORDER - 65 AUTOANTIBODY',
'GENERALIZED ANXIETY DISORDER 65 S') then 'Glutamic acid decarboxylase-65 antibody (GAD65)'
when proc_desc in ('INSULIN ANTIBODIES',
'INSULIN ANTIBODIES ; SERUM',
'INSULIN ANTIBIOTICS',
'HEMOGLOBIN INSULIN ANTIBODY',
'HC INSULIN ANTIBODIES',
'INSULIN ANTIBODIES MAYO M 8666',
'HC INSULIN ABS',
'INSULIN ANTIBODIES - S',
'LB - INSULIN ANTIBODIES',
'HEMOGLOBIN INSULIN AUTOANTIBODIES',
'HEMOGLOBIN INSULIN ANTIBODIES',
'ANTI- INSULIN ANTIBODIES - R/L',
'INSULIN ; ANTIBODIES',
'HC INSULIN AB',
'HC INSULIN AUTOANTIBODIES',
'INSULIN AUTOANTIBODIES - 141598',
'INSULIN AUTOANTIBODIES',
'INSULIN ANTIBODY',
'INSULIN AB',
'INSULIN ANTIBODIES SERUM',
'INSULIN AUTOANTIBODY') then 'Insulin antibody'
when proc_desc in ('IA - 2 ANTIBODY',
'IA - 2 AUTOANTIBODIES',
'HEMOGLOBIN IA - 2 ANTIBODY',
'IA - 2 AB',
'IA2 AUTOANTIBODIES - 141531') then 'Islet antigen 2 antibody (IA-2A)'
when proc_desc in ('HC ISLET CELL ANTIBODY','ISLET CELL ANTIBODY IGG',
'HEMOGLOBIN ISLET CELL ANTIBODY ; IGG',
'ISLET CELL AB',
'ANTI-ISLET CELL ANTIBODY',
'HC ISLET CELL AB',
'ISLET CELL ANTIBODY ; IGG',
'HEMOGLOBIN ISLET CELL ANTIBODY','ISLET CELL ANTIBODY') then 'Pancreatic islet cell antibody (ICA)'
else 'Others' end as Test_flag
 from ac_ehr_islet_proc_202305_1
where proc_desc in ('ISLET CELL ANTIBODY IGG',
'INSULIN ANTIBODIES',
'INSULIN ANTIBODIES ; SERUM',
'INSULIN ANTIBIOTICS',
'HEMOGLOBIN INSULIN ANTIBODY',
'GENERALIZED ANXIETY DISORDER 65 ; IA - 2 ; AND INSULIN AUTOANTIBODY',
'HC INSULIN ANTIBODIES',
'INSULIN ANTIBODIES MAYO M 8666',
'HC ISLET CELL ANTIBODY',
'HEMOGLOBIN ISLET CELL ANTIBODY ; IGG',
'HC INSULIN ABS',
'IA - 2 ANTIBODY',
'HC GENERALIZED ANXIETY DISORDER - 65 AB',
'ISLET CELL AB',
'GENERALIZED ANXIETY DISORDER 65',
'IA - 2 AUTOANTIBODIES',
'INSULIN ANTIBODIES - S',
'LB - INSULIN ANTIBODIES',
'HEMOGLOBIN INSULIN AUTOANTIBODIES',
'HEMOGLOBIN INSULIN ANTIBODIES',
'ANTI- INSULIN ANTIBODIES - R/L',
'ANTI-ISLET CELL ANTIBODY',
'HC ISLET CELL AB',
'HC GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'GLUTAMIC ACID DECARBOXYLASE',
'INSULIN ; ANTIBODIES',
'HC INSULIN AB',
'ISLET CELL ANTIBODY ; IGG',
'HC INSULIN AUTOANTIBODIES',
'HEMOGLOBIN IA - 2 ANTIBODY',
'IA - 2 AB',
'CHG GLUTAMIC ACID DECARBOXYLASE AB GAD65',
'GENERALIZED ANXIETY DISORDER 65 ANTIBODY',
'HEMOGLOBIN ISLET CELL ANTIBODY',
'INSULIN AUTOANTIBODIES - 141598',
'GENERALIZED ANXIETY DISORDER - 65 AUTOANTIBODY',
'INSULIN AUTOANTIBODIES',
'INSULIN ANTIBODY',
'INSULIN AB',
'INSULIN ANTIBODIES SERUM',
'ISLET CELL ANTIBODY',
'INSULIN AUTOANTIBODY',
'IA2 AUTOANTIBODIES - 141531',
'GENERALIZED ANXIETY DISORDER 65 S') and
proc_date >='2015-07-01' and proc_date<='2023-01-31'
order by ptid, service_date;

select distinct * from ac_ehr_AA_tests_lab_proc_full
order by ptid, service_date;


-- COMMAND ----------

drop table if exists ac_ehr_islet_lab_proc_bl_fu_test_cnt;
create or replace table ac_ehr_islet_lab_proc_bl_fu_test_cnt as
select distinct ptid, count(distinct test_name) as test_cnt from ac_ehr_AA_tests_lab_proc_full
-- where service_date >='2015-07-01' and service_date<='2023-01-31'
group by 1
order by 1;

select distinct * from ac_ehr_islet_lab_proc_bl_fu_test_cnt
order by 2 desc;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_AA_test_lab_proc;
-- select count(distinct ptid) from ac_ehr_islet_lab_proc_bl_fu_test_cnt;

-- COMMAND ----------

-- create or replace table ac_ehr_patient_202305_full_pts as
-- select distinct a.*,left(FIRST_MONTH_ACTIVE,4) as first_mnth_yr, right(FIRST_MONTH_ACTIVE,2) as first_mnth,
-- left(LAST_MONTH_ACTIVE,4) as last_mnth_yr, right(LAST_MONTH_ACTIVE,2) as last_mnth from ac_ehr_patient_202305 a;

-- select distinct * from ac_ehr_patient_202305_full_pts;

create or replace table ac_ehr_patient_202305_full_pts_2 as
select distinct *, cast(concat(first_mnth_yr,'-',first_mnth,'-','01') as date) as first_month_active_new,
cast(concat(last_mnth_yr,'-',last_mnth,'-','01') as date) as last_month_active_new from ac_ehr_patient_202305_full_pts;

select distinct * from ac_ehr_patient_202305_full_pts_2;


-- COMMAND ----------

select '01 baseline cov' as step, count(distinct a.ptid) as cnts from ac_ehr_islet_lab_16_22_indx a
inner join ac_ehr_patient_202305_full_pts_2 b on a.ptid=b.ptid
where (first_month_active_new<=index_date - 180 and index_date +30 <=last_month_active_new); -- 80314

-- COMMAND ----------

create or replace table ac_ehr_lab_proc_attr as
select '01 baseline cov' as step, count(distinct a.ptid) as cnts from ac_ehr_islet_lab_proc_16_22_indx a
left join ac_ehr_patient_202305_full_pts_2 b on a.ptid=b.ptid
where first_month_active_new<=index_date - 180 and index_date - 1<=last_month_active_new
union
select '02 follow up cov' as step, count(distinct a.ptid) as cnts from ac_ehr_islet_lab_proc_16_22_indx a
left join ac_ehr_patient_202305_full_pts_2 b on a.ptid=b.ptid
where (first_month_active_new<=index_date - 180 and index_date +30 <=last_month_active_new)
union
select '03 atleast 2 AA tests' as step, count(distinct a.ptid) as cnts from ac_ehr_islet_lab_proc_bl_fu_test_cnt a
left join ac_ehr_islet_lab_proc_16_22_indx ind on a.ptid=ind.ptid
left join ac_ehr_patient_202305_full_pts_2 b on a.ptid=b.ptid
where a.test_cnt>=2 and (first_month_active_new<=ind.index_date - 180 and ind.index_date +30 <=last_month_active_new)
order by step;

select distinct * from ac_ehr_lab_proc_attr
order by step;


-- COMMAND ----------

create or replace table ac_ehr_AA_tsts_final as
select distinct a.ptid, ind.index_date, a.test_cnt from ac_ehr_islet_lab_bl_fu_test_cnt a
inner join ac_ehr_islet_lab_16_22_indx ind on a.ptid=ind.ptid
inner join ac_ehr_patient_202305_full_pts_2 b on a.ptid=b.ptid
where a.test_cnt>=2 and (first_month_active_new<=ind.index_date - 180 and ind.index_date +30 <=last_month_active_new);

select distinct * from ac_ehr_AA_tsts_final
order by 1;


-- COMMAND ----------

-- select distinct ptid, count(distinct index_date) as cnt from ac_ehr_AA_tsts_final
-- group by 1
-- order by 2 desc;
select test_cnt, count(distinct ptid) from ac_ehr_AA_tsts_final
where test_cnt>4
group by 1
order by 1;

-- COMMAND ----------

select year(index_date) as indx_yr, count(distinct ptid) as cnts from ac_ehr_AA_tsts_final
group by 1
order by 1;


-- COMMAND ----------

-- MAGIC %md #### test at index date

-- COMMAND ----------

select distinct * from ac_ehr_AA_tsts_final

-- COMMAND ----------

create or replace table ac_ehr_AA_tst_indx as
select distinct a.*, b.index_date from ac_ehr_islet_lab_16_22_1 a
inner join ac_ehr_AA_tsts_final b on a.ptid=b.ptid and a.service_date=b.index_date
order by a.ptid, a.service_date, a.test_name;

select distinct * from ac_ehr_AA_tst_indx
order by ptid, service_date;

-- COMMAND ----------

select distinct test_name, count(distinct ptid) from ac_ehr_AA_tst_indx
group by 1
order by 1;


-- COMMAND ----------

create or replace table ac_ehr_AA_tst_final_table as
select distinct a.*, b.index_date from ac_ehr_islet_lab_16_22_1 a
inner join ac_ehr_AA_tsts_final b on a.ptid=b.ptid 
order by a.ptid, a.service_date;

select distinct * from ac_ehr_AA_tst_final_table
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_AA_test_rank as
select distinct *
from (select distinct *, dense_rank() OVER (PARTITION BY ptid ORDER BY service_date) as rank
from (select distinct ptid, service_date, test_name from ac_ehr_AA_tst_final_table))
order by ptid
;

select distinct * from ac_ehr_AA_test_rank
order by ptid, service_date, rank;

PT078207412

-- COMMAND ----------

-- MAGIC %md #### second test after index

-- COMMAND ----------

create or replace table ac_ehr_AA_tets_scnd_aftr_indx as
select distinct a.*, b.rank from ac_ehr_AA_tst_final_table a
inner join (select distinct * from ac_ehr_AA_test_rank where rank=2) b on a.ptid=b.ptid and a.service_date=b.service_date
order by a.ptid, a.service_date, a.test_name;

select distinct * from ac_ehr_AA_tets_scnd_aftr_indx
order by ptid, service_date;

-- COMMAND ----------

select distinct test_name, count(distinct ptid) from ac_ehr_AA_tets_scnd_aftr_indx
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### test combinations

-- COMMAND ----------

create or replace table ac_ehr_AA_tst_indx_1 as
select distinct ptid, concat_ws('--', collect_list(test_name)) as index_test from ac_ehr_AA_tst_indx
group by 1
order by 1;

select distinct * from ac_ehr_AA_tst_indx_1
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_AA_tets_scnd_aftr_indx_1 as
select distinct ptid, concat_ws('--', collect_list(test_name)) as second_test from ac_ehr_AA_tets_scnd_aftr_indx
group by 1
order by 1;

select distinct * from ac_ehr_AA_tets_scnd_aftr_indx_1
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_AA_tst_combintn as
select distinct a.ptid, concat(a.index_test, ' AND ',b.second_test) as test from ac_ehr_AA_tst_indx_1 a
inner join ac_ehr_AA_tets_scnd_aftr_indx_1 b on a.ptid=b.ptid
order by 1;

select distinct * from ac_ehr_AA_tst_combintn
order by ptid;

-- COMMAND ----------

select distinct test, count(distinct ptid) from ac_ehr_AA_tst_combintn
group by 1
order by 2 desc;

-- COMMAND ----------

-- MAGIC %md ###baseline t2d treatment

-- COMMAND ----------

create or replace table ac_ehr_AA_tst_bl_t2d_trmnt as
select distinct a.*, b.index_date  from ac_rx_anti_dm a
inner join ac_ehr_AA_tsts_final b on a.ptid=b.ptid
where a.rxdate>= b.index_date - 180 and a.rxdate<=b.index_date - 1
order by a.ptid, a.rxdate;

select distinct * from ac_ehr_AA_tst_bl_t2d_trmnt
order by ptid, rxdate;

-- COMMAND ----------

select distinct count(distinct ptid) from ac_ehr_AA_tst_bl_t2d_trmnt


-- COMMAND ----------

select distinct rx_type, count(distinct ptid) from ac_ehr_AA_tst_bl_t2d_trmnt
group by 1
order by 2 desc;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md ####baseline hba1c data

-- COMMAND ----------

create or replace table ac_ehr_AA_tst_bl_t2d_hba1c as
select distinct a.*, b.index_date  from ac_ehr_lab_antibody_name_value a
inner join ac_ehr_AA_tsts_final b on a.ptid=b.ptid
where a.service_date>= b.index_date - 180 and a.service_date<=b.index_date - 1
order by a.ptid, a.service_date;

select distinct * from ac_ehr_AA_tst_bl_t2d_hba1c
order by ptid, service_date;

-- COMMAND ----------

select distinct count(distinct ptid) from ac_ehr_AA_tst_bl_t2d_hba1c;
-- select distinct count(distinct ptid) from ac_ehr_AA_tst_bl_t2d_hba1c
-- where value <5.7;
-- select distinct count(distinct ptid) from ac_ehr_AA_tst_bl_t2d_hba1c
-- where value between 5.7 and 6.4;
-- select distinct count(distinct ptid) from ac_ehr_AA_tst_bl_t2d_hba1c
-- where value >= 6.5;

-- COMMAND ----------

select distinct category from sg_antidiabetics_codes;

-- select distinct * from ty00_ses_rx_anti_dm_loopup 

-- COMMAND ----------


select distinct test_name from ac_ehr_lab_202305
where lower(test_name) like '%dysglycemia%' or lower(test_name) like '%glucose%' or lower(test_name) like '%hemoglobin%' or
lower(test_name) like '%glycosylated%';

-- COMMAND ----------

create or replace  table ac_ehr_bl_ogtt_aa_tests as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date,cast(test_result as double) as result, b.index_date
from ac_ehr_lab_202305 a
inner join ac_ehr_AA_tsts_final b
on a.ptid=b.ptid
where 
test_name in ('Glucose.tolerance test.3 hour','Glucose.tolerance test.1 hour','Glucose.tolerance test.2 hour') and
coalesce(a.result_date,a.collected_date,a.order_date)>= b.index_date - 180 and coalesce(a.result_date,a.collected_date,a.order_date)<=b.index_date - 1 ;

select distinct * from ac_ehr_bl_ogtt_aa_tests
order by ptid, service_date;

-- COMMAND ----------

drop table if exists ac_ehr_bl_ogtt_aa_tests_name_value;

create table ac_ehr_bl_ogtt_aa_tests_name_value as
select *, case when result>0 then result
when isnotnull(result) then result
when test_result =    '"6.1""%"' then 6.1
when test_result =    '"7.2 % ' "" ' / ' / "" ' / /"' then 7.2
when test_result =    "'6.7%" then 6.7
when test_result =    "* 6.1 ( 4.8 - 5.9 )" then 6.1
when test_result =    "+14.0%" then 14.0
when test_result =    "+15%" then 15.0
when test_result =    ". > 14.0 %" then 14.1
when test_result =    ". hemoglobin a1c = 5.9" then 5.9
when test_result =    ".14.0%" then 14.0
when test_result =    ".4.7 %" then  4.7
when test_result =    ".6.2%" then  6.2
when test_result =    ".6.7%" then  6.7
when test_result =    ".9.1%" then  9.1
when test_result =    "10.%" then  10.0
when test_result =    "10.'3" then  10.3
when test_result =    "10.+%" then  10.1
when test_result =    "10..0" then  10.0
when test_result =    "10..2" then  10.2
when test_result =    "10..4" then  10.4
when test_result =    "10.0 h ( 4.5 - 5.7 )" then  10.
when test_result =    "10.0%+" then  10.
when test_result =    "10.0&" then  10.
when test_result =    "10.0+" then  10.
when test_result =    "10.0." then  10.
when test_result =    "10.1%+" then  10.2
when test_result =    "10.1-h" then  10.1
when test_result =    "10.1." then  10.1
when test_result =    "10.1b" then  10.1
when test_result =    "10.1f" then  10.1
when test_result =    "10.1h" then  10.1
when test_result =    "10.2%+" then  10.2
when test_result =    "10.2&" then  10.2
when test_result =    "10.2*h" then  10.2
when test_result =    "4.6l %" then  4.61
when test_result =    "4.7&" then  4.7
when test_result like '<10.o' then 9.9
when substr(test_result,5,1)='%' then cast(substr(test_result,1,4) as double)
when test_result like '<%' then cast(substr(test_result,2) as double)-0.1
when test_result like '-%' then cast(substr(test_result,2) as double)
when test_result like '.%' then cast(substr(test_result,2) as double)
when test_result like '%%' then cast(substr(test_result,2) as double)
when test_result like '<^%' then cast(substr(test_result,3) as double)-0.1
when test_result like '>%' then cast(substr(test_result,2) as double)+0.1
when test_result like '.>%' then cast(substr(test_result,3) as double)+0.1
else null end value
from ac_ehr_bl_ogtt_aa_tests
;

-- select count(*) as n_obs
-- from ac_lab_antibody_name_value
-- where isnull(result) and isnotnull(value)
-- ;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_bl_ogtt_aa_tests_name_value;

select mean(value) from ac_ehr_bl_ogtt_aa_tests_name_value;

select std(value) from ac_ehr_bl_ogtt_aa_tests_name_value;


-- COMMAND ----------

create or replace table ac_ehr_ogtt_proc_bl_aa_tst as
select distinct a.*, b.PROC_CODE, b.PROC_DESC, b.PROC_DATE  from ac_ehr_AA_tsts_final a
left join ac_ehr_proc_202305 b on a.ptid=b.ptid
where b.PROC_DATE between a.index_date - 180 and a.index_date - 1 
and b.PROC_CODE in ('82947', '82950','82951','82952')
order by a.ptid;

select distinct * from ac_ehr_ogtt_proc_bl_aa_tst 
order by ptid;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_ogtt_proc_bl_aa_tst
where PROC_CODE in ('82951','82952');

select distinct ptid from ac_ehr_bl_ogtt_aa_tests_name_value
union
select distinct ptid from ac_ehr_ogtt_proc_bl_aa_tst
where PROC_CODE in ('82951','82952')
