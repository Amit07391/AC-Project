-- Databricks notebook source
select distinct * from sg_antidiabetics_codes;

-- COMMAND ----------

drop table if exists ac_ehr_bl_rx_pres_AA_tst;

create table ac_ehr_bl_rx_pres_AA_tst as
select distinct a.ptid, a.rxdate, a.drug_name, a.ndc, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.daily_dose, a.dose_frequency, a.quantity_per_fill
, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, b.index_date,
case when c.category is not null then c.category
else 'Others' end as Drug_flag
from ac_ehr_WRx_202305 a join ac_ehr_AA_tsts_final b
on a.ptid=b.ptid
left join sg_antidiabetics_codes c on a.ndc=c.ndc_code
where a.rxdate>= b.index_date - 180 and a.rxdate<=b.index_date - 1
order by a.ptid, a.rxdate
;

select * from ac_ehr_bl_rx_pres_AA_tst
order by ptid, rxdate;


-- COMMAND ----------

drop table if exists ac_ehr_bl_rx_admi_AA_tst;

create table ac_ehr_bl_rx_admi_AA_tst as
select distinct a.ptid, a.admin_date as rxdate, a.drug_name, a.ndc, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.dose_frequency
, a.generic_desc, a.drug_class, b.index_date,
case when c.category is not null then c.category
else 'Others' end as Drug_flag
from ac_ehr_202305_RX_Administration a join ac_ehr_AA_tsts_final b
on a.ptid=b.ptid
left join sg_antidiabetics_codes c on a.ndc=c.ndc_code
where a.admin_date>= b.index_date - 180 and a.admin_date<=b.index_date - 1
order by a.ptid, rxdate
;

select * from ac_ehr_bl_rx_admi_AA_tst;


-- COMMAND ----------

drop table if exists ac_ehr_rx_AA_test;

create table ac_ehr_rx_AA_test as
select ptid, rxdate, ndc, drug_name, drug_class, generic_desc,drug_flag, 'Pres' as source from ac_ehr_bl_rx_pres_AA_tst
union
select ptid, rxdate, ndc, drug_name, drug_class, generic_desc,drug_flag, 'Admi' as source from ac_ehr_bl_rx_admi_AA_tst
order by ptid, rxdate
;

select * from ac_ehr_rx_AA_test;



-- COMMAND ----------

select count(distinct ptid) from ac_ehr_rx_AA_test where ptid not in (select distinct ptid from ac_ehr_rx_AA_test
where drug_flag in ('Basal_Insulin','Bolus_Insulin','GLP-1','OAD','Premix_Insulin') );

-- select drug_flag, count(distinct ptid) from ac_ehr_rx_AA_test
-- group by 1
-- order by 1;

-- COMMAND ----------

-- MAGIC %md #### Checking demographic data

-- COMMAND ----------

create or replace table ac_ehr_bl_aa_tst_demo as
select distinct a.*, b.gender, b.BIRTH_YR, b.DIVISION, b.ETHNICITY, b.RACE, b.REGION from ac_ehr_AA_tsts_final a
left join  ac_ehr_patient_202305 b
on a.ptid=b.ptid
order by a.ptid;

select distinct * from ac_ehr_bl_aa_tst_demo;

-- COMMAND ----------



select count(distinct ptid) from ac_ehr_bl_aa_tst_demo
where DIVISION is not null

-- COMMAND ----------

create or replace table ac_ehr_bl_AA_tst_ins as
select distinct a.*, b.INS_TYPE   from ac_ehr_AA_tsts_final a
left join ac_ehr_insurance_202305 b on a.ptid=b.ptid
where b.INSURANCE_DATE between a.index_date - 180 and a.index_date - 1;

select distinct * from ac_ehr_bl_AA_tst_ins ;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_bl_AA_tst_ins
where ins_type is not null

-- COMMAND ----------

create or replace table ac_ehr_bl_AA_tst_bmi_obs as
select distinct a.*, b.obs_type   from ac_ehr_AA_tsts_final a
left join ac_ehr_obs_202305 b on a.ptid=b.ptid
where b.OBS_DATE between a.index_date - 180 and a.index_date - 1 and obs_type='BMI';

select distinct * from ac_ehr_bl_AA_tst_bmi_obs ;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_bl_AA_tst_bmi_obs


-- COMMAND ----------

create or replace table ac_ehr_bl_AA_tst_enc_prov as
select distinct a.*, b.specialty, b.PROVID, eprov.PROVIDER_ROLE  from ac_ehr_AA_tst_final_table a
left join ac_ehr_enc_202305 enc on a.ptid=enc.ptid
left join ac_ehr_enc_prov_202305 eprov on enc.encid=eprov.encid
left join ac_ehr_prov_202305 b on eprov.provid=b.provid
where enc.INTERACTION_DATE between a.index_date - 180 and a.index_date - 1;

select distinct * from ac_ehr_bl_AA_tst_enc_prov ;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_bl_AA_tst_enc_prov
where provid is not null

-- COMMAND ----------

create or replace table ac_ehr_bl_AA_tst_prov as
select distinct a.*  from ac_ehr_AA_tsts_final a
left join ac_ehr_enc_202305 b a.ptid=b.ptid
inner join ac_ehr_prov_202305 b on a.ptid=b.ptid
where b.NOTE_DATE between a.index_date - 180 and a.index_date - 1;

select distinct * from ac_ehr_bl_AA_tst_prov ;


-- COMMAND ----------

-- MAGIC %md #### C peptide test

-- COMMAND ----------

create or replace table ac_ehr_Cpep_bl_aa_tst as
select distinct a.*, b.PROC_CODE, b.PROC_DESC, b.PROC_DATE  from ac_ehr_AA_tsts_final a
left join ac_ehr_proc_202305 b on a.ptid=b.ptid
where b.PROC_DATE between a.index_date - 180 and a.index_date - 1 
and b.PROC_CODE in ('83525', '84681')
order by a.ptid;

select distinct * from ac_ehr_Cpep_bl_aa_tst 
order by ptid;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_Cpep_bl_aa_tst

-- COMMAND ----------

select distinct test_name from ac_ehr_lab_202305
where lower(test_name) like '%peptide%'

-- COMMAND ----------

create or replace table ac_ehr_bl_AA_tst_cpep_lab as
select distinct a.*, coalesce(b.result_date,b.collected_date,b.order_date) as service_date from ac_ehr_AA_tsts_final a
left join ac_ehr_lab_202305 b on a.ptid=b.ptid
where (coalesce(b.result_date,b.collected_date,b.order_date) between a.index_date - 180 and a.index_date - 1)
and test_name in ('C-peptide.random','C-peptide.fasting','C-peptide.post-challenge')
order by a.ptid;

select distinct * from ac_ehr_bl_AA_tst_cpep_lab
order by ptid;

-- COMMAND ----------

-- create or replace table ac_ehr_cpep_bl_pts as
-- select distinct ptid, proc_code from ac_ehr_Cpep_bl_aa_tst
-- union
-- select distinct ptid , 'lab'  from ac_ehr_bl_AA_tst_cpep_lab;

-- select count(distinct ptid) from ac_ehr_cpep_bl_pts;

select proc_code, count(distinct ptid) from ac_ehr_cpep_bl_pts
group by 1
order by 1;

-- COMMAND ----------

select distinct test_name from ac_ehr_lab_202305
where lower(test_name) like '%leukocyte%' or lower(test_name) like '%hla%' or lower(test_name) like '%human leukocyte antigen%'

-- COMMAND ----------

create or replace table ac_ehr_bl_AA_tst_HLA_lab as
select distinct a.*, coalesce(b.result_date,b.collected_date,b.order_date) as service_date from ac_ehr_AA_tsts_final a
left join ac_ehr_lab_202305 b on a.ptid=b.ptid
where (coalesce(b.result_date,b.collected_date,b.order_date) between a.index_date - 180 and a.index_date - 1)
and test_name in ('HLA antigen typing.unspecified specimen')
order by a.ptid;

select distinct * from ac_ehr_bl_AA_tst_HLA_lab
order by ptid;

-- COMMAND ----------

create or replace table ac_ehr_HLA_bl_proc_aa_tst as
select distinct a.*, b.PROC_CODE, b.PROC_DESC, b.PROC_DATE  from ac_ehr_AA_tsts_final a
left join ac_ehr_proc_202305 b on a.ptid=b.ptid
where b.PROC_DATE between a.index_date - 180 and a.index_date - 1 
and b.PROC_CODE in ('81370',
'81371',
'81372',
'81373',
'81374',
'81375',
'81376',
'81377',
'81378',
'81379',
'81380',
'81381',
'81382',
'81383')
order by a.ptid;

select distinct * from ac_ehr_HLA_bl_proc_aa_tst 
order by ptid;

-- COMMAND ----------

-- create or replace table ac_ehr_HLA_bl_pts as
-- select distinct ptid, 'lab' as test from ac_ehr_bl_AA_tst_HLA_lab
-- union
-- select distinct ptid, proc_code from ac_ehr_HLA_bl_proc_aa_tst;

select count(distinct ptid) from ac_ehr_HLA_bl_pts;

select test, count(distinct ptid) from ac_ehr_HLA_bl_pts
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_fu_AA_tst_enc as
select distinct a.*, enc.INTERACTION_TYPE  from ac_ehr_AA_tst_final_table a
left join ac_ehr_enc_202305 enc on a.ptid=enc.ptid
where enc.INTERACTION_DATE between a.index_date  and a.index_date +30;

select distinct * from ac_ehr_fu_AA_tst_enc ;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_fu_AA_tst_enc
where INTERACTION_TYPE is not null

-- COMMAND ----------

create or replace table ac_ehr_full_act_AA_tsts as
select distinct a.*, b.first_month_active_new, b.last_month_active_new from ac_ehr_AA_tst_final_table a
left join ac_ehr_patient_202305_full_pts_2 b on a.ptid=b.ptid
order by a.ptid;

select distinct * from ac_ehr_full_act_AA_tsts
order by ptid;

-- COMMAND ----------

-- create or replace table ac_ehr_full_act_AA_tsts_2 as
-- select distinct *, year(last_month_active_new) - year(first_month_active_new) as diff_yr from ac_ehr_full_act_AA_tsts
-- order by ptid;

-- select distinct * from ac_ehr_full_act_AA_tsts_2
-- order by ptid;

select mean(diff_yr) as mn, std(diff_yr) as stdev 
from (select distinct ptid, diff_yr from ac_ehr_full_act_AA_tsts_2);



-- COMMAND ----------

-- create or replace table ac_ehr_full_act_AA_tsts_aftr_indx as
-- select distinct *, year(last_month_active_new) - year(index_date) as diff_yr from ac_ehr_full_act_AA_tsts
-- order by ptid;

-- select distinct * from ac_ehr_full_act_AA_tsts_aftr_indx
-- order by ptid;

select mean(diff_yr) as mn, std(diff_yr) as stdev 
from (select distinct ptid, diff_yr from ac_ehr_full_act_AA_tsts_aftr_indx);

-- COMMAND ----------

-- MAGIC %md #### Positive AA test results

-- COMMAND ----------

create or replace table ac_ehr_AA_tst_final_table_2 as
select distinct *, cast(test_result as double) as result from ac_ehr_AA_tst_final_table
order by ptid, service_date;

select distinct * from ac_ehr_AA_tst_final_table_2
order by ptid, service_date;

-- COMMAND ----------

-- select distinct * from ac_ehr_AA_tst_final_table

-- drop table if exists ac_ehr_aa_tests_name_value;

create table ac_ehr_aa_tests_name_value as
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
from ac_ehr_AA_tst_final_table_2
;


select distinct * from ac_ehr_aa_tests_name_value
order by ptid, service_date;


-- COMMAND ----------

select distinct test_name from ac_ehr_aa_tests_name_value

-- COMMAND ----------

create table ac_ehr_AA_lab_antibody_posit as
select distinct *
from ac_ehr_aa_tests_name_value
where (test_name like '%IA-2A%' and value>=7.5 and (isnull(result_unit) or result_unit='u/ml'))
   or (test_name like '%ICA%' and lcase(test_result) rlike
      '^(1 : 128|1 : 256|1 : 64|1 : 8|1-128|1-16|1-256|1-32|1-64|1-8|1:1024|1:128|1:16|1:2048|1:256|1:32|1:4096|1:512|1:64|1:8|< 1 : 4|< 1:4|<1:4|<1:64|=1:128|positive)')
   or (test_name like '%ZnT8%' and value>15 and (isnull(result_unit) or result_unit='u/ml'))
   or (test_name like '%GAD65%' and value>5 and (isnull(result_unit) or result_unit in ('u/ml','no units','iuml','iu/ml^iu/ml','text units','underscore')))
   or (test_name like 'Insulin antibody' and value>0.4 and (isnull(result_unit) or result_unit in ('u/ml','no units','iuml','iu/ml^iu/ml','text units','underscore')))
order by ptid, service_date;
;

select distinct * from ac_ehr_AA_lab_antibody_posit
order by ptid, service_date;
;

-- COMMAND ----------

select distinct value from ac_ehr_AA_lab_antibody_posit
where test_name like '%Insulin antibody%'

-- COMMAND ----------

-- select distinct test_name, count(distinct ptid) from ac_ehr_AA_lab_antibody_posit
-- group by 1
-- order by 1;

select distinct  count(distinct ptid) from ac_ehr_aa_tests_name_value;

-- COMMAND ----------

create table ac_ehr_2305_sds_family_aa_test as
select distinct a.*,b.index_date,
case when lcase(sds_family_member) like '%mother of child%' then 'Parent'
     when lcase(sds_family_member) like '%mother :& father%' then 'Parent'
     when lcase(sds_family_member) like '%paternal aunt%' then 'Grandparent'
     when lcase(sds_family_member) like '%paternal uncle%' then 'Grandparent'
     when lcase(sds_family_member) like '%ancestors%' then 'Grandparent'
     when lcase(sds_family_member) like '%parent%' then 'Parent'
     when lcase(sds_family_member) like '%paternal%' then 'Parent'
     when lcase(sds_family_member) like '%maternal grandmother%' then 'Grandparent'
     when lcase(sds_family_member) like '%grandmother%' then 'Grandparent'
     when lcase(sds_family_member) like '%son mother%' then 'Parent'
     when lcase(sds_family_member) like '%mother%' then 'Parent'
     when lcase(sds_family_member) like '%maternal grandfather%' then 'Grandparent'
     when lcase(sds_family_member) like '%grandfather%' then 'Grandparent'
     when lcase(sds_family_member) like '%father%' then 'Parent'
     when lcase(sds_family_member) like '%foster parent%' then 'Parent'
     when lcase(sds_family_member) like '%grandparent%' then 'Grandparent'
     when lcase(sds_family_member) like '%husband :& son%' then 'Husband :& Son'
     when lcase(sds_family_member) like '%husband%' then 'Husband'
     when lcase(sds_family_member) like '%spouse%' then 'Spouse'
     when lcase(sds_family_member) like '%maternal%' then 'Wife'
     when lcase(sds_family_member) like '%wife%' then 'Wife'
     when lcase(sds_family_member) like '%wives%' then 'Wife'
     when lcase(sds_family_member) like '%son%' then 'Offspring'
     when lcase(sds_family_member) like '%daughter%' then 'Offspring'
     when lcase(sds_family_member) like '%girl%' then 'Offspring'
     when lcase(sds_family_member) like '%adolescent%' then 'Offspring'
     when lcase(sds_family_member) like '%baby%' then 'Offspring'
     when lcase(sds_family_member) like '%toddler%' then 'Offspring'
     when lcase(sds_family_member) like '%babies%' then 'Offspring'
     when lcase(sds_family_member) like '%biological children%' then 'Offspring'
     when lcase(sds_family_member) like '%child%' then 'Offspring'
     when lcase(sds_family_member) like '%youngster%' then 'Offspring'
     when lcase(sds_family_member) like '%infant%' then 'Offspring'
     when lcase(sds_family_member) like '%maternal boy%' then 'Offspring'
     when lcase(sds_family_member) like '%boy%' then 'Offspring'
     when lcase(sds_family_member) like '%teen%' then 'Offspring'
     when lcase(sds_family_member) like '%nephew%' then 'Offspring'
     when lcase(sds_family_member) like '%niece%' then 'Offspring'
     when lcase(sds_family_member) like '%sibling%' then 'Sibling'
     when lcase(sds_family_member) like '%fraternal%' then 'Sibling'
     when lcase(sds_family_member) like '%sororal%' then 'Sibling'
     when lcase(sds_family_member) like '%twin%' then 'Identical twin'
     when lcase(sds_family_member) like '%triplet%' then 'Triplet'
     when lcase(sds_family_member) like '%cousin%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%1st-degree relative%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%1st-degree;relative%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%1st-degree male relative%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%half brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%close relatives brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%biological relatives brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%relative :& brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%fraternal twin maternal relatives%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%brother :& (only close relative%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%primary relative%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%maternal relatives%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%male relatives brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%brother :& relative%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%relatives :& brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%relatives brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%1st :& 2nd-degree%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%1st-degree%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%1st%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%brother maternal relative%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%biological relatives) :& brother%' then 'Sibling and another first degree relative'
     when lcase(sds_family_member) like '%2nd-degree%' then 'Second degree relative'
     when lcase(sds_family_member) like '%2nd%' then 'Second degree relative'
     when lcase(sds_family_member) like "%brother :'s wife%" then 'Second degree relative'
     when lcase(sds_family_member) like '%brother%' then 'Sibling'
     when lcase(sds_family_member) like '%sister%' then 'Sibling'
     when lcase(sds_family_member) like '%3rd-degree%' then 'Third degree relative or further removed'
     when lcase(sds_family_member) like '%3rd%' then 'Third degree relative or further removed'
     else 'Third degree relative or further removed' end as family_member_normal
from ac_ehr_2305_sds_family a
inner join ac_ehr_AA_tst_final_table b on a.ptid=b.ptid
order by a.ptid, a.NOTE_DATE
;

select distinct * from ac_ehr_2305_sds_family_aa_test
order by ptid, note_date;

-- COMMAND ----------

create or replace table ac_ehr_2305_sds_family_aa_test_2 as
select distinct * from ac_ehr_2305_sds_family_aa_test
where NOTE_DATE between index_date - 180 and index_date - 1
order by ptid, NOTE_DATE;

select distinct * from ac_ehr_2305_sds_family_aa_test_2
order by ptid, note_date;

-- COMMAND ----------

select family_member_normal, count(distinct ptid) from ac_ehr_2305_sds_family_aa_test_2
group by 1
order by 2 desc;

select  count(distinct ptid) from ac_ehr_2305_sds_family_aa_test_2;
