-- Databricks notebook source
-- drop table if exists ac_ehr_WRx_202303;
-- CREATE TABLE ac_ehr_WRx_202303 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202303/ontology/base/RX Prescribed";

-- select distinct * from ac_ehr_WRx_202303;

drop table if exists ac_ehr_2303_RX_Administration;

CREATE TABLE ac_ehr_2303_RX_Administration USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/RX Administration';

select * from ac_ehr_2303_RX_Administration;

-- COMMAND ----------

select max(diag_date) from ac_ehr_diag_202303

-- COMMAND ----------

drop table if exists ac_ehr_dx_t2dm_full;
create or replace table ac_ehr_dx_t2dm_full as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE, b.dx_name
from ac_ehr_diag_202303 a inner join ty00_all_dx_comorb b on a.DIAGNOSIS_CD=b.code
where a.diag_date>='2014-07-01' AND
b.dx_name='T2DM'
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_t2dm_full
order by ptid, diag_date;

-- COMMAND ----------

drop table if exists ac_ehr_dx_t1dm_secnd_full;
create or replace table ac_ehr_dx_t1dm_secnd_full as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE
from ac_ehr_diag_202303 a
where a.diag_date>='2014-07-01' AND
(DIAGNOSIS_CD in (select code from ty00_all_dx_comorb where dx_name='T1DM')
or DIAGNOSIS_CD like '249%' or DIAGNOSIS_CD like 'E08%' or DIAGNOSIS_CD like 'E09%')
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_t1dm_secnd_full
order by ptid, diag_date;

-- COMMAND ----------

select distinct diag from ty19_dx_subset_17_22
where dx_name='T1DM'

-- COMMAND ----------

select distinct drug_name, ndc, generic_desc from ac_ehr_WRx_202303
where lcase(GENERIC_DESC) like '%glargine%'

-- COMMAND ----------

drop table if exists ac_rx_pres_anti_dm;

create table ac_rx_pres_anti_dm as
select distinct a.ptid, a.rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.daily_dose, a.dose_frequency, a.quantity_per_fill
, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, b.*
from ac_ehr_WRx_202303 a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, a.rxdate
;

select * from ac_rx_pres_anti_dm;


-- COMMAND ----------

drop table if exists ac_rx_admi_anti_dm;

create table ac_rx_admi_anti_dm as
select distinct a.ptid, a.admin_date as rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.dose_frequency
, a.generic_desc, a.drug_class, b.*
from ac_ehr_2303_RX_Administration a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, rxdate
;

select * from ac_rx_admi_anti_dm;


-- COMMAND ----------

drop table if exists ac_rx_anti_dm;

create table ac_rx_anti_dm as
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm, 'Pres' as source from ac_rx_pres_anti_dm
union
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm, 'Admi' as source from ac_rx_admi_anti_dm
order by ptid, rxdate
;

select * from ac_rx_anti_dm;

select rx_type, drug_name
from ac_rx_anti_dm
group by rx_type, drug_name
order by rx_type, drug_name
;


-- COMMAND ----------

drop table if exists ac_pat_rx_basal_nph_glp1_test;

create table ac_pat_rx_basal_nph_glp1_test as
select distinct ptid, rxdate,drug_name,generic_desc,ndc,rx_type,gnrc_nm,a.brnd_nm,
       case when lcase(drug_name) like '%toujeo%' then 'Toujeo'
              when lcase(rx_type) like '%basal%' and lcase(drug_name) not like '%toujeo%' then 'Other long-acting BIs'
              when lcase(rx_type) like '%glp1%' then 'GLP1'
                   else rx_type end as rx_type2
from ac_rx_pres_anti_dm a
where RXDATE>='2013-01-01' and (lcase(rx_type) in ('basal', 'bolus', 'premix') or lcase(gnrc_nm) in ('dulaglutide','semaglutide','exenatide', 'exenatide microspheres'))
order by a.PTID, a.RXDATE
;

select rx_type2, rx_type, GENERIC_DESC,DRUG_NAME, min(RXDATE) as dt_rx_start, max(RXDATE) as dt_rx_end
from ac_pat_rx_basal_nph_glp1_test
group by rx_type2, rx_type, GENERIC_DESC,DRUG_NAME
order by rx_type2, rx_type, GENERIC_DESC,DRUG_NAME
;



-- COMMAND ----------

select distinct * from ac_pat_rx_basal_nph_glp1
order by ptid, rxdate

-- COMMAND ----------

drop table if exists ac_pat_dx_rx_test;

create table ac_pat_dx_rx_test as
select distinct a.ptid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_toujeo) as dt_1st_toujeo
         , min(d.dt_1st_other_bi) as dt_rx_other_insulin
        , case when isnotnull(min(c.dt_1st_toujeo)) then min(c.dt_1st_toujeo)
               else min(d.dt_1st_other_bi) end as dt_rx_index
        , case when isnotnull(min(c.dt_1st_toujeo)) then 'Toujeo'
               when isnotnull(min(d.dt_1st_other_bi)) then 'Other long-acting BIs'
               else null end as index_group
from (select distinct ptid, min(diag_date) as dt_1st_t2dm, count(distinct diag_date) as n_t2dm from ac_ehr_dx_t2dm_full
where diag_date between '2014-07-01' and '2021-12-31' group by ptid) a
      left join (select distinct ptid, min(diag_date) as dt_1st_t1dm, count(distinct diag_date) as n_t1dm from ac_ehr_dx_t1dm_secnd_full where diag_date between '2014-07-01' and '2021-12-31' group by ptid) b on a.ptid=b.ptid
      left join (select distinct ptid, min(rxdate) as dt_1st_toujeo from ac_pat_rx_basal_nph_glp1_test where rxdate between '2015-01-01' and  '2021-12-31' and rx_type2 in ('Toujeo') group by ptid) c on a.ptid=c.ptid
      left join (select distinct ptid, min(rxdate) as dt_1st_other_bi from ac_pat_rx_basal_nph_glp1_test where rxdate between '2015-01-01' and  '2021-12-31' and rx_type2 in ('Other long-acting BIs') group by ptid) d on a.ptid=d.ptid
group by a.ptid
order by a.ptid
;

select count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_t2dm) as dt_d2dm_start, max(dt_1st_t2dm) as dt_d2dm_end
from ac_pat_dx_rx
where isnotnull(dt_rx_index)
;

select index_group, count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_rx_index) as dt_rx_start, max(dt_rx_index) as dt_rx_end
from ac_pat_dx_rx
group by index_group
;


-- COMMAND ----------

drop table if exists ac_pat_rx_basal_nph_glp1_index_test;

create table ac_pat_rx_basal_nph_glp1_index_test as
select distinct a.*, b.dt_rx_index
from ac_pat_rx_basal_nph_glp1_test a left join ac_pat_dx_rx_test b
on a.ptid=b.ptid
order by a.ptid, a.rxdate
;

select * from ac_pat_rx_basal_nph_glp1_index_test;


-- COMMAND ----------

create or replace  table ac_ehr_t1d_lab_202303 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date, cast(test_result as double) as result
from ac_ehr_lab_202303 a 
where test_name='Hemoglobin A1C' ;

select distinct * from ac_ehr_t1d_lab_202303
order by ptid, service_date;

-- COMMAND ----------

select test_result, count(*) as n_obs from ac_ehr_t1d_lab_202303
--where lcase(tst_desc) like '%a1c%'
group by test_result
order by 2 desc

-- COMMAND ----------

drop table if exists ac_lab_antibody_name_value;

create table ac_lab_antibody_name_value as
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
from ac_ehr_t1d_lab_202303
;

select count(*) as n_obs
from ac_lab_antibody_name_value
where isnull(result) and isnotnull(value)
;


-- COMMAND ----------

drop table if exists ac_lab_a1c_index_test;

create table ac_lab_a1c_index_test as
select a.*, b.dt_rx_index
from ac_ehr_t1d_lab_202303 a join ac_pat_dx_rx_test b
on a.ptid=b.ptid
where isnotnull(b.dt_rx_index) and a.result between 3 and 15
order by a.ptid, a.service_date
;

select * from ac_lab_a1c_index_test;

-- select test_result, count(*) as n_obs from ac_lab_a1c_index
-- --where lcase(tst_desc) like '%a1c%'
-- group by test_result
-- order by 2 desc


-- COMMAND ----------

select distinct ptid, min(rxdate) as min_dt, max(rxdate) as dt_last_glp1_bl from ac_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and rxdate between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1) group by ptid

-- COMMAND ----------

drop table if exists ac_glp1_a1c_bl_fu_test;

create table ac_glp1_a1c_bl_fu_test as
select distinct a.ptid as patid1, max(b.dt_last_glp1_bl) as dt_last_glp1_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, min(e.dt_1st_glp1_fu) as dt_1st_glp1_fu, max(f.dt_last_insulin_bl) as dt_last_insulin_bl
from (select ptid, dt_rx_index from ac_pat_dx_rx where isnotnull(dt_rx_index)) a
     left join (select distinct ptid, max(rxdate) as dt_last_glp1_bl from ac_pat_rx_basal_nph_glp1_index_test where lcase(rx_type) in ('glp1') and rxdate between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1) group by ptid) b on a.ptid=b.ptid
     left join (select distinct ptid, max(service_date) as dt_last_a1c_bl from ac_lab_a1c_index_test where service_date between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) c on a.ptid=c.ptid
     left join (select distinct ptid, min(service_date) as dt_1st_a1c_fu from ac_lab_a1c_index_test where service_date between dt_rx_index and dt_rx_index + 180  group by ptid) d on a.ptid=d.ptid
     left join (select distinct ptid, min(rxdate) as dt_1st_glp1_fu from ac_pat_rx_basal_nph_glp1_index_test where lcase(rx_type) in ('glp1') and rxdate between dt_rx_index and dt_rx_index + 180 group by ptid) e on a.ptid=e.ptid
     left join (select distinct ptid, max(rxdate) as dt_last_insulin_bl from ac_pat_rx_basal_nph_glp1_index_test where lcase(rx_type) not in ('glp1') and rxdate between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) f on a.ptid=f.ptid
group by patid1
order by patid1
;

select * from ac_glp1_a1c_bl_fu_test;

-- COMMAND ----------

drop table if exists ac_pat_all_enrol_test;

create table ac_pat_all_enrol_test as
select distinct a.*, 
case when index_group='Toujeo' then index_group
                          when index_group='Other long-acting BIs' then 'Other long-acting BIs'
                          else null end as index_group2
                   , case when index_group='Toujeo' then dt_rx_index
                          when index_group='Other long-acting BIs' then dt_rx_index
                          else null end as dt_rx_index2,
b.*, c.first_month_active_new , c.last_month_active_new, c.gender, c.birth_yr,year(a.dt_rx_index) as yr_indx, case when c.BIRTH_YR='1933 and Earlier' then 1933 else cast(c.birth_yr as int) end as yrdob
from ac_pat_dx_rx_test a left join ac_glp1_a1c_bl_fu_test b on a.ptid=b.patid1
                      left join ac_ehr_patient_202303_full_pts_1 c on a.ptid=c.ptid and a.dt_rx_index between c.first_month_active_new and c.last_month_active_new
order by a.ptid
;

select * from ac_pat_all_enrol_test;

create or replace table ac_pat_all_enrol_tst_2 as
select distinct * , yr_indx - yrdob as Age_index from ac_pat_all_enrol_test
order by ptid;

select * from ac_pat_all_enrol_tst_2;

-- COMMAND ----------

drop table if exists ac_patient_attrition_toujeo_test;

create table ac_patient_attrition_toujeo_test as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where dt_1st_t2dm between '2014-07-01' and '2021-12-31'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_tst_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
-- union
-- select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ac_patient_attrition_toujeo_test;


-- COMMAND ----------


drop table if exists ac_patient_attrition_gla100;

create table ac_patient_attrition_gla100 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where dt_1st_t2dm between '2014-07-01' and '2021-12-31'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100))
-- union
-- select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ac_pat_all_enrol_2 where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_gla_100)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ac_patient_attrition_gla100;


-- COMMAND ----------


drop table if exists ac_patient_attrition_detemir;

create table ac_patient_attrition_detemir as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where dt_1st_t2dm between '2014-07-01' and '2021-12-31'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir))
order by Step
;

select * from ac_patient_attrition_detemir;


-- COMMAND ----------


drop table if exists ac_patient_attrition_NPH;

create table ac_patient_attrition_NPH as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where dt_1st_t2dm between '2014-07-01' and '2021-12-31'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph))
order by Step
;

select * from ac_patient_attrition_NPH;


-- COMMAND ----------

drop table if exists ac_patient_attrition_other;

create table ac_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where dt_1st_t2dm between '2014-07-01' and '2021-12-31'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_1st_t2dm between '2014-07-01' and '2021-12-31' and dt_rx_index between '2015-01-01' and '2021-12-31'

order by Step
;

select * from ac_patient_attrition_other;

