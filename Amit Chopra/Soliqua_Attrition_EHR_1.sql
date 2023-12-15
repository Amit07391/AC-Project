-- Databricks notebook source
-- drop table if exists ac_ehr_WRx_202305;
-- CREATE TABLE ac_ehr_WRx_202305 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202305/ontology/base/RX Prescribed";

-- select distinct * from ac_ehr_WRx_202305;

-- drop table if exists ac_ehr_202305_RX_Administration;

-- CREATE TABLE ac_ehr_202305_RX_Administration USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202305/ontology/base/RX Administration';

-- select * from ac_ehr_202305_RX_Administration;


-- drop table if exists ac_ehr_diag_202305;

-- CREATE TABLE ac_ehr_diag_202305 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202305/ontology/base/Diagnosis';

-- select * from ac_ehr_diag_202305;

drop table if exists ac_ehr_lab_202305;

CREATE TABLE ac_ehr_lab_202305 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202305/ontology/base/Lab';

select * from ac_ehr_lab_202305;

-- COMMAND ----------

select max(diag_date) from ac_ehr_diag_202305

-- COMMAND ----------

drop table if exists ac_ehr_dx_t2dm_16_22;
create or replace table ac_ehr_dx_t2dm_16_22 as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE, b.dx_name
from ac_ehr_diag_202305 a inner join ty00_all_dx_comorb b on a.DIAGNOSIS_CD=b.code
where a.diag_date>='2016-07-01' AND a.diag_date<='2022-06-30' AND
b.dx_name='T2DM'
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_t2dm_16_22
order by ptid, diag_date;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_t2dm_16_22

-- COMMAND ----------

-- create or replace table ac_test_T2D_sol as
-- select distinct a.* from ac_rx_anti_dm a
-- inner join ac_ehr_dx_t2dm_16_22 b on a.ptid=b.ptid
-- where lower(a.brnd_nm) like '%soliqua%'
-- and a.rxdate>='2017-01-01' AND a.rxdate<='2021-12-31';

-- select count(distinct ptid) from ac_test_T2D_sol;

-- COMMAND ----------

create or replace table ac_ehr_dx_t2d_sol_diag_cnt as
select distinct ptid, count(distinct diag_date) as diag_cnt 
from ac_ehr_dx_t2dm_16_22
group by 1
order by 1;

select distinct * from ac_ehr_dx_t2d_sol_diag_cnt
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_dx_t2d_sol_1 as
select distinct a.ptid, a.diag_date, lead(diag_date) over (partition by a.ptid order by diag_date) as next_dt, dense_rank() OVER (PARTITION BY a.ptid ORDER BY diag_date) as rank, b.diag_cnt
from ac_ehr_dx_t2dm_16_22 a
inner join ac_ehr_dx_t2d_sol_diag_cnt b on a.ptid=b.ptid
order by 1,2;

select distinct * from ac_ehr_dx_t2d_sol_1
order by 1,2;

-- COMMAND ----------

create or replace table ac_ehr_dx_t2d_sol_2 as
select distinct *, date_diff(next_dt, diag_date) as diff from ac_ehr_dx_t2d_sol_1
where diag_cnt>=2
order by 1;

select distinct * from ac_ehr_dx_t2d_sol_2
order by 1,2;


-- COMMAND ----------


create or replace table ac_ehr_dx_t2d_sol_3 as
select distinct * from ac_ehr_dx_t2d_sol_2
where diff>=30
order by 1,2;

select distinct * from ac_ehr_dx_t2d_sol_3
order by 1,2;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_t2d_sol_3

-- COMMAND ----------

create or replace table ac_ehr_dx_t2d_sol_4 as
select distinct a.* from ac_ehr_dx_t2dm_16_22 a
inner join ac_ehr_dx_t2d_sol_3 b on a.ptid=b.ptid
order by a.ptid, a.diag_date;

select distinct * from ac_ehr_dx_t2d_sol_4
order by ptid, diag_date;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_t2d_sol_4

-- COMMAND ----------

drop table if exists ac_ehr_dx_t1dm_secnd_16_22;
create or replace table ac_ehr_dx_t1dm_secnd_16_22 as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE
from ac_ehr_diag_202305 a
where a.DIAG_DATE>='2016-07-01' AND a.DIAG_DATE<='2022-06-30' AND
(DIAGNOSIS_CD in (select code from ty00_all_dx_comorb where dx_name='T1DM')
or DIAGNOSIS_CD like '249%' or DIAGNOSIS_CD like 'E08%' or DIAGNOSIS_CD like 'E09%')
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_t1dm_secnd_16_22
order by ptid, diag_date;

-- COMMAND ----------

select distinct diag from ty19_dx_subset_17_22
where dx_name='T1DM'

-- COMMAND ----------

select distinct drug_name, ndc, generic_desc from ac_ehr_WRx_202303
where lcase(GENERIC_DESC) like '%glargine%'

-- COMMAND ----------

select distinct ndc from ty00_ses_rx_anti_dm_loopup
where lower(brnd_nm) like '%soliqua%'

-- COMMAND ----------

drop table if exists ac_rx_pres_anti_dm;

create table ac_rx_pres_anti_dm as
select distinct a.ptid, a.rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.daily_dose, a.dose_frequency, a.quantity_per_fill
, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, b.*
from ac_ehr_WRx_202305 a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, a.rxdate
;

select * from ac_rx_pres_anti_dm;


-- COMMAND ----------

drop table if exists ac_rx_admi_anti_dm;

create table ac_rx_admi_anti_dm as
select distinct a.ptid, a.admin_date as rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.dose_frequency
, a.generic_desc, a.drug_class, b.*
from ac_ehr_202305_RX_Administration a join ty00_ses_rx_anti_dm_loopup b
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

drop table if exists ac_ehr_pat_rx_bas_bol_sol;

create table ac_ehr_pat_rx_bas_bol_sol as
select distinct ptid, rxdate,drug_name,generic_desc,ndc,rx_type,gnrc_nm,a.brnd_nm
from ac_rx_anti_dm a
where rxdate>='2016-01-01' AND rxdate<='2022-06-30' and lcase(rx_type) in ('basal', 'bolus')
order by a.PTID, a.RXDATE
;

select distinct * from ac_ehr_pat_rx_bas_bol_sol
order by PTID, RXDATE;



-- COMMAND ----------

drop table if exists ac_ehr_pat_dx_rx_sol;

create table ac_ehr_pat_dx_rx_sol as
select distinct a.ptid,min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_Basal) as dt_1st_Basal, min(d.dt_1st_Bolus) as dt_1st_Bolus
from (select distinct ptid, min(diag_date) as dt_1st_t2dm, count(distinct diag_date) as n_t2dm from ac_ehr_dx_t2d_sol_4 group by ptid) a
      left join (select distinct ptid, min(diag_date) as dt_1st_t1dm, count(distinct diag_date) as n_t1dm from ac_ehr_dx_t1dm_secnd_16_22 group by ptid) b on a.ptid=b.ptid
      left join (select distinct ptid, min(rxdate) as dt_1st_Basal from ac_ehr_pat_rx_bas_bol_sol WHERE rx_type in ('Basal') and rxdate>='2016-07-01' AND rxdate<='2022-06-30' group by ptid) c on a.ptid=c.ptid
      left join (select distinct ptid, min(rxdate) as dt_1st_Bolus from ac_ehr_pat_rx_bas_bol_sol WHERE rx_type in ('Bolus') and rxdate>='2016-07-01' AND rxdate<='2022-06-30' group by ptid) d on a.ptid=d.ptid
group by a.ptid
order by a.ptid
;

select distinct * from ac_ehr_pat_dx_rx_sol
order by 1;

-- COMMAND ----------

create or replace  table ac_ehr_t2d_lab_202305 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date, cast(test_result as double) as result
from ac_ehr_lab_202305 a 
where test_name='Hemoglobin A1C' ;

select distinct * from ac_ehr_t2d_lab_202305
order by ptid, service_date;

-- COMMAND ----------

select test_result, count(*) as n_obs from ac_ehr_t2d_lab_202305
--where lcase(tst_desc) like '%a1c%'
group by test_result
order by 2 desc

-- COMMAND ----------

drop table if exists ac_ehr_lab_antibody_name_value;

create table ac_ehr_lab_antibody_name_value as
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
from ac_ehr_t2d_lab_202305
;

-- select count(*) as n_obs
-- from ac_lab_antibody_name_value
-- where isnull(result) and isnotnull(value)
-- ;


-- COMMAND ----------

select distinct * from ac_ehr_lab_antibody_name_value

-- COMMAND ----------

drop table if exists ac_ehr_2305_lab_a1c_sol_1;

create table ac_ehr_2305_lab_a1c_sol_1 as
select a.*
from ac_ehr_lab_antibody_name_value a join ac_ehr_pat_dx_rx_sol b
on a.ptid=b.ptid
where a.value between 5 and 15
order by a.ptid, a.service_date
;

select * from ac_ehr_2305_lab_a1c_sol_1;

create or replace table ac_ehr_2305_lab_a1c_sol_2 as
select distinct * from ac_ehr_2305_lab_a1c_sol_1
where value>=8 and service_date>='2017-01-01' AND service_date<='2021-12-31'
order by ptid, service_date;

select * from ac_ehr_2305_lab_a1c_sol_2
order by ptid, service_date;


-- COMMAND ----------

create or replace table ac_ehr_2305_lab_a1c_sol_index as
select distinct ptid, min(service_date) as indx_a1c_dt 
from ac_ehr_2305_lab_a1c_sol_2 
group by 1
order by 1;


select distinct * from ac_ehr_2305_lab_a1c_sol_index
order by 1;

create or replace table ac_ehr_2305_lab_a1c_sol_3 as
select distinct a.*, b.indx_a1c_dt 
from ac_ehr_2305_lab_a1c_sol_2 a
inner join ac_ehr_2305_lab_a1c_sol_index b on a.ptid=b.ptid
order by a.ptid, a.service_date;

select distinct * from ac_ehr_2305_lab_a1c_sol_3
order by ptid, service_date;


-- COMMAND ----------

-- MAGIC %md #### Checking baseline bolus and basal use

-- COMMAND ----------

create or replace table ac1206_ehr_bas_bol_bl_use as
select distinct a.*, b.indx_a1c_dt
from ac_ehr_pat_rx_bas_bol_sol a 
inner join ac_ehr_2305_lab_a1c_sol_3 b on a.ptid=b.ptid
where a.rxdate between b.indx_a1c_dt - 180 and b.indx_a1c_dt - 1
order by a.ptid, a.rxdate;

select distinct * from ac1206_ehr_bas_bol_bl_use
where ptid='PT597623974'
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac1206_ehr_bas_bl_use as
select distinct ptid, rxdate, rx_type, brnd_nm from
ac1206_ehr_bas_bol_bl_use
where rx_type='Basal'
order by 1,2;

-- COMMAND ----------

-- create or replace table ac1206_ehr_bol_bl_use as
-- select distinct ptid, rxdate from
-- ac1206_ehr_bas_bol_bl_use
-- where rx_type='Bolus'
-- order by 1,2;

create or replace table ac1206_ehr_bol_bl_use_tst as
select distinct ptid, rxdate, brnd_nm  from
ac1206_ehr_bas_bol_bl_use
where rx_type='Bolus'
order by 1,2;

select distinct * from ac1206_ehr_bol_bl_use_tst
order by 1,2;

-- COMMAND ----------

create or replace table ac1206_ehr_bol_bl_use_1 as
select distinct ptid, rxdate,brnd_nm,  'Bolus' as rx_type, lead(rxdate) over(partition by ptid order by rxdate) as Next_dt from
ac1206_ehr_bol_bl_use_tst
order by 1,2;

select distinct * from ac1206_ehr_bol_bl_use_1
order by 1,2;

-- COMMAND ----------

-- create or replace table ac1206_bas_bol_bl_use_2 as
-- select distinct a.*, c.brnd_nm, b.rxdate as rx_dt_bas, b.brnd_nm as bas_brnd_nm  from ac1206_ehr_bol_bl_use_1 a
-- join ac1206_ehr_bas_bl_use b on a.ptid=b.ptid
-- left join ac1206_ehr_bol_bl_use_tst c on a.ptid=c.ptid and a.rxdate=c.rxdate
-- where b.rxdate between a.rxdate and a.next_dt
-- order by a.ptid, a.rxdate;

-- select distinct * from ac1206_bas_bol_bl_use_2
-- order by ptid, rxdate;

create or replace table ac1206_bas_bol_bl_use_testing as
select distinct a.*, b.rxdate as rx_dt_bas, b.brnd_nm as bas_brnd_nm  from ac1206_ehr_bol_bl_use_1 a
join ac1206_ehr_bas_bl_use b on a.ptid=b.ptid
where b.rxdate between a.rxdate and a.next_dt
order by a.ptid, a.rxdate;

select distinct * from ac1206_bas_bol_bl_use_testing
where ptid='PT078158621'
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac1206_ehr_bas_bol_bl_use_final as
select distinct a.* from ac1206_ehr_bas_bol_bl_use a
inner join ac1206_bas_bol_bl_use_testing b on a.ptid=b.ptid
order by a.ptid, a.rxdate;

select distinct * from ac1206_ehr_bas_bol_bl_use_final
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac1206_ehr_bas_bol_bl_use
where lower(brnd_nm) like '%soliqua%'

-- COMMAND ----------

create or replace table ac1206_ehr_bas_bol_bl_use_concat as
select distinct ptid, concat(brnd_nm, '--', bas_brnd_nm) as str from ac1206_bas_bol_bl_use_testing 
order by ptid;

select distinct * from ac1206_ehr_bas_bol_bl_use_concat
order by ptid;

select distinct str, count(distinct ptid) from ac1206_ehr_bas_bol_bl_use_concat
group by 1
order by 2 desc;

-- COMMAND ----------

drop table if exists ac_ehr_pat_rx_basal_bolus_index;

create table ac_ehr_pat_rx_basal_bolus_index as
select distinct a.*, b.indx_a1c_dt
from ac_ehr_pat_rx_bas_bol_sol a inner join ac_ehr_2305_lab_a1c_sol_index b
on a.ptid=b.ptid
order by a.ptid, a.rxdate
;

select * from ac_ehr_pat_rx_basal_bolus_index;

-- COMMAND ----------

drop table if exists ac_ehr_bas_bol_sol_bl;

create table ac_ehr_bas_bol_sol_bl as
select distinct a.ptid as ptid1, max(b.dt_last_bas_bol_bl) as dt_last_bas_bol_bl, max(c.dt_last_sol_bl) as dt_last_sol_bl
from (select ptid, indx_a1c_dt from ac_ehr_2305_lab_a1c_sol_3 ) a
     left join (select distinct ptid, max(rxdate) as dt_last_bas_bol_bl from ac1206_ehr_bas_bol_bl_use_final group by ptid) b on a.ptid=b.ptid
     left join (select distinct ptid, max(rxdate) as dt_last_sol_bl from ac_ehr_pat_rx_basal_bolus_index where (rxdate between date_sub(indx_a1c_dt,180) and date_sub(indx_a1c_dt,1)) and lower(BRND_NM) like '%soliqua%' group by ptid) c on a.ptid=c.ptid
group by ptid1
order by ptid1
;

select * from ac_ehr_bas_bol_sol_bl;

-- COMMAND ----------

select distinct ptid, max(rxdate) as dt_last_sol_bl from ac_ehr_pat_rx_basal_bolus_index where (rxdate between date_sub(indx_a1c_dt,180) and date_sub(indx_a1c_dt,1)) and lower(BRND_NM) like '%soliqua%' group by ptid

-- COMMAND ----------

create or replace table pat_ehr_all_sol as
select distinct a.*, b.* from ac_ehr_pat_dx_rx_sol a
left join ac_ehr_bas_bol_sol_bl b on a.ptid=b.ptid1;

select distinct * from pat_ehr_all_sol
-- where isnotnull(dt_last_sol_bl)
order by ptid;

-- COMMAND ----------

drop table if exists ac_ehr_patient_attrition_sol;

create table ac_ehr_patient_attrition_sol as
select ' 1. At least two diagnoses of T2D (30 days apart) during the study period' as Step, count(distinct ptid) as n_pat from pat_ehr_all_sol where isnotnull(dt_1st_t2dm)
union
select ' 2. Include patients with at least one basal insulin prescription and one bolus insulin prescription during the study period' as Step, count(distinct ptid) as n_pat from pat_ehr_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus)
union
select ' 3. Include patients with one or more valid HbA1c measurment and HbA1c >=8% during the study period.' as Step, count(distinct ptid) as n_pat from pat_ehr_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(ptid1)
union
select ' 4. Include patients with at least 30 days overlap between basal insulin and bolus insulin (BB) during the baseline period (6 months prior to the index date)' as Step, count(distinct ptid) as n_pat from pat_ehr_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(ptid1) and isnotnull(dt_last_bas_bol_bl)
union
select ' 5. Exclude (1) Diagnoses of T1D at any time during the study period (2) Diagnosis of secondary diabetes during the study period (3) with iGlarLixi prescriptions during the baseline period' as Step, count(distinct ptid) as n_pat from pat_ehr_all_sol where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(ptid1) and isnotnull(dt_last_bas_bol_bl) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_sol_bl)
order by Step
;

select * from ac_ehr_patient_attrition_sol;
