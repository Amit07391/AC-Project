-- Databricks notebook source
-- drop table if exists ac_ehr_WRx_202308;
-- CREATE TABLE ac_ehr_WRx_202308 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202308/ontology/base/RX Prescribed";

-- select distinct * from ac_ehr_WRx_202308;

-- drop table if exists ac_ehr_202308_RX_Administration;

-- CREATE TABLE ac_ehr_202308_RX_Administration USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202308/ontology/base/RX Administration';

-- select * from ac_ehr_202308_RX_Administration;


-- drop table if exists ac_ehr_diag_202308;

-- CREATE TABLE ac_ehr_diag_202308 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202308/ontology/base/Diagnosis';

-- select * from ac_ehr_diag_202308;

-- drop table if exists ac_ehr_lab_202308;

-- CREATE TABLE ac_ehr_lab_202308 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202308/ontology/base/Lab';

-- select * from ac_ehr_lab_202308;

-- drop table if exists ac_ehr_lab_202308;

-- CREATE TABLE ac_ehr_lab_202308 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202308/ontology/base/Lab';

-- select * from ac_ehr_lab_202308;

-- drop table if exists ac_ehr_obs_202308;

-- CREATE TABLE ac_ehr_obs_202308 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202308/ontology/base/Observation';

-- select * from ac_ehr_obs_202308;



-- drop table if exists ac_ehr_enc_202308;

-- CREATE TABLE ac_ehr_enc_202308 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202308/ontology/base/Encounter';

-- select * from ac_ehr_enc_202308;

drop table if exists ac_ehr_Visit_202308;

CREATE TABLE ac_ehr_Visit_202308 USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202308/ontology/base/Visit';

select * from ac_ehr_Visit_202308;

-- COMMAND ----------

select max(ORDER_DATE) from ac_ehr_202308_RX_Administration

-- COMMAND ----------

select distinct * from ac_ehr_WRx_202305

-- COMMAND ----------

drop table if exists ac_ehr_dx_t2dm_16_23;
create or replace table ac_ehr_dx_t2dm_16_23 as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE, b.dx_name
from ac_ehr_diag_202308 a inner join ty00_all_dx_comorb b on a.DIAGNOSIS_CD=b.code
where a.diag_date>='2016-07-01' AND a.diag_date<='2023-03-31' AND
b.dx_name='T2DM'
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_t2dm_16_23
order by ptid, diag_date;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_dx_t2dm_16_23  --5551223

-- COMMAND ----------

drop table if exists ac_ehr_dx_t1dm_secnd_16_23;
create or replace table ac_ehr_dx_t1dm_secnd_16_23 as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE
from ac_ehr_diag_202308 a
where a.diag_date>='2016-07-01' AND a.diag_date<='2023-03-31' AND
(DIAGNOSIS_CD in (select code from ty00_all_dx_comorb where dx_name='T1DM')
or DIAGNOSIS_CD like '249%' or DIAGNOSIS_CD like 'E08%' or DIAGNOSIS_CD like 'E09%')
and DIAGNOSIS_STATUS='Diagnosis of';

select distinct * from ac_ehr_dx_t1dm_secnd_16_23
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

drop table if exists ac_rx_pres_anti_dm_16_23;

create table ac_rx_pres_anti_dm_16_23 as
select distinct a.ptid, a.rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.daily_dose, a.dose_frequency, a.quantity_per_fill
, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, b.*
from ac_ehr_wrx_202308 a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, a.rxdate
;

select * from ac_rx_pres_anti_dm_16_23;


-- COMMAND ----------

drop table if exists ac_rx_admi_anti_dm_16_23;

create table ac_rx_admi_anti_dm_16_23 as
select distinct a.ptid, a.admin_date as rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.dose_frequency
, a.generic_desc, a.drug_class, b.*
from ac_ehr_202308_RX_Administration a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, rxdate
;

select * from ac_rx_admi_anti_dm_16_23;


-- COMMAND ----------

select distinct * from ac_rx_admi_anti_dm_16_23
where rxdate>'2023-04-15'

-- COMMAND ----------

drop table if exists ac_rx_anti_dm_16_23;

create table ac_rx_anti_dm_16_23 as
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm, 'Pres' as source from ac_rx_pres_anti_dm_16_23
union
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm, 'Admi' as source from ac_rx_admi_anti_dm_16_23
order by ptid, rxdate
;

select * from ac_rx_anti_dm_16_23;

-- select rx_type, drug_name
-- from ac_rx_anti_dm
-- group by rx_type, drug_name
-- order by rx_type, drug_name
-- ;


-- COMMAND ----------

create or replace table ac_ehr_pat_rx_16_23_bas_bol_sol as
select distinct * from ac_rx_anti_dm_16_23
where RXDATE>='2015-01-01'
order by ptid, rxdate;

select distinct * from ac_ehr_pat_rx_16_23_bas_bol_sol

order by ptid, rxdate;


-- COMMAND ----------

select distinct * from ac_ehr_pat_rx_16_23_bas_bol_sol where rxdate between '2017-01-01' and  '2022-09-30' and CATEGORY like ('%iGlarLixi%') 

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

drop table if exists ac_pat_dx_rx_sol_16_23;

create table ac_pat_dx_rx_sol_16_23 as
select distinct a.ptid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_soliqua) as dt_rx_index, min(d.dt_1st_sec_diab) as dt_1st_sec_diab
from (select distinct ptid, min(diag_date) as dt_1st_t2dm, count(distinct diag_date) as n_t2dm from ac_ehr_dx_t2dm_16_23
where diag_date between '2016-07-01' and '2023-03-31' group by ptid) a
      left join (select distinct ptid, min(diag_date) as dt_1st_t1dm, count(distinct diag_date) as n_t1dm from ac_ehr_dx_t1dm_secnd_16_23 where diag_date between '2016-07-01' and '2023-03-31' and DIAGNOSIS_CD in (select code from ty00_all_dx_comorb where dx_name='T1DM') group by ptid) b on a.ptid=b.ptid
       left join (select distinct ptid, min(diag_date) as dt_1st_sec_diab from ac_ehr_dx_t1dm_secnd_16_23 where diag_date between '2016-07-01' and '2023-03-31' and (DIAGNOSIS_CD like '249%' or DIAGNOSIS_CD like 'E08%' or DIAGNOSIS_CD like 'E09%') group by ptid) d on a.ptid=d.ptid
      left join (select distinct ptid, min(rxdate) as dt_1st_soliqua from ac_ehr_pat_rx_16_23_bas_bol_sol where rxdate between '2017-01-01' and  '2022-09-30' and lcase(brnd_nm) like ('%soliqua%') group by ptid) c on a.ptid=c.ptid
group by a.ptid
order by a.ptid
;

select distinct *
from ac_pat_dx_rx_sol_16_23
where isnotnull(dt_rx_index)
;


-- COMMAND ----------

drop table if exists ac_ehr_pat_rx_16_23_bas_bol_sol_indx;

create table ac_ehr_pat_rx_16_23_bas_bol_sol_indx as
select distinct a.*, b.dt_rx_index
from ac_ehr_pat_rx_16_23_bas_bol_sol a left join ac_pat_dx_rx_sol_16_23 b 
on a.ptid=b.ptid
order by a.ptid, a.rxdate
;

select * from ac_ehr_pat_rx_16_23_bas_bol_sol_indx;


-- COMMAND ----------

create or replace  table ac_ehr_t2d_lab_202308 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date, cast(test_result as double) as result
from ac_ehr_lab_202308 a 
where test_name='Hemoglobin A1C' ;

select distinct * from ac_ehr_t2d_lab_202308
order by ptid, service_date;

-- COMMAND ----------

select test_result, count(*) as n_obs from ac_ehr_t2d_lab_202305
--where lcase(tst_desc) like '%a1c%'
group by test_result
order by 2 desc

-- COMMAND ----------

drop table if exists ac_ehr_lab_antibody_name_16_23_value;

create table ac_ehr_lab_antibody_name_16_23_value as
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
from ac_ehr_t2d_lab_202308
;

-- select count(*) as n_obs
-- from ac_lab_antibody_name_value
-- where isnull(result) and isnotnull(value)
-- ;


-- COMMAND ----------

select distinct * from ac_ehr_lab_antibody_name_16_23_value

-- COMMAND ----------

drop table if exists ac_ehr_2308_lab_a1c_sol_index;

create table ac_ehr_2308_lab_a1c_sol_index as
select a.*,b.dt_rx_index
from ac_ehr_lab_antibody_name_16_23_value a join ac_pat_dx_rx_sol_16_23 b
on a.ptid=b.ptid
where isnotnull(b.dt_rx_index) and a.value between 5 and 15
order by a.ptid, a.service_date
;

select distinct * from ac_ehr_2308_lab_a1c_sol_index
order by ptid, service_date;


-- COMMAND ----------

create or replace table ac_ehr_pat_rx_bas_bol_index_only as
select distinct * from ac_ehr_pat_rx_bas_bol_sol_index
where lcase(rx_type) in ('basal', 'bolus') and rxdate between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1)
order by ptid, rxdate;

select distinct * from ac_ehr_pat_rx_bas_bol_index_only
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_ehr_pat_rx_bas_bol_overlap as
select distinct a.* from ac_ehr_pat_rx_bas_bol_index_only a
join ac_ehr_pat_rx_bas_bol_index_only b on a.ptid=b.ptid and a.rx_type<>b.rx_type
where ((a.rxdate between b.rxdate and b.rxdate + 30 ) OR
(b.rxdate between a.rxdate and a.rxdate + 30 ))
order by a.ptid, a.rxdate;

select distinct * from ac_ehr_pat_rx_bas_bol_overlap
order by ptid, rxdate;


-- COMMAND ----------

select distinct * from ac_ehr_pat_rx_bas_bol_overlap_test
where ptid not in (select distinct ptid from ac_ehr_pat_rx_bas_bol_overlap )
order by ptid, rxdate;

-- COMMAND ----------

drop table if exists ac_ehr_sol_16_23_a1c_bl_fu;

create table ac_ehr_sol_16_23_a1c_bl_fu as
select distinct a.ptid as patid1, max(b.dt_last_bas_bl) as dt_last_bas_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, max(e.dt_last_sol_bl) as dt_last_sol_bl, max(f.dt_last_bolus_bl) as dt_last_bolus_bl, min(g.dt_1st_bas_bol_fu) as dt_1st_bas_bol_fu, min(h.dt_1st_bol_fu) as dt_1st_bol_fu, min(sol.dt_1st_sol_fu) as dt_1st_sol_fu
from (select ptid, dt_rx_index from ac_pat_dx_rx_sol_16_23 where isnotnull(dt_rx_index)) a
     left join (select distinct ptid, max(rxdate) as dt_last_bas_bl from ac_ehr_pat_rx_16_23_bas_bol_sol_indx where lcase(rx_type) in ('basal') and rxdate between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) b on a.ptid=b.ptid
     left join (select distinct ptid, max(service_date) as dt_last_a1c_bl from ac_ehr_2308_lab_a1c_sol_index where service_date between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) c on a.ptid=c.ptid
     left join (select distinct ptid, min(service_date) as dt_1st_a1c_fu from ac_ehr_2308_lab_a1c_sol_index where service_date between dt_rx_index + 90 and dt_rx_index + 210  group by ptid) d on a.ptid=d.ptid
     left join (select distinct ptid, max(rxdate) as dt_last_sol_bl from ac_ehr_pat_rx_16_23_bas_bol_sol_indx where lcase(brnd_nm) like ('%soliqua%') and rxdate between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) e on a.ptid=e.ptid
     left join (select distinct ptid, min(rxdate) as dt_1st_sol_fu from ac_ehr_pat_rx_16_23_bas_bol_sol_indx where lcase(brnd_nm) like ('%soliqua%') and rxdate between dt_rx_index + 90 and dt_rx_index + 210 group by ptid) sol on a.ptid=sol.ptid
     left join (select distinct ptid, max(rxdate) as dt_last_bolus_bl from ac_ehr_pat_rx_16_23_bas_bol_sol_indx where lcase(rx_type)  in ('bolus') and rxdate between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) f on a.ptid=f.ptid
     left join (select distinct ptid, min(rxdate) as dt_1st_bas_bol_fu from ac_ehr_pat_rx_16_23_bas_bol_sol_indx where lcase(rx_type)  in ('basal','bolus') and rxdate between dt_rx_index + 1 and dt_rx_index + 180 group by ptid) g on a.ptid=g.ptid
     left join (select distinct ptid, min(rxdate) as dt_1st_bol_fu from ac_ehr_pat_rx_16_23_bas_bol_sol_indx where lcase(rx_type)  in ('bolus') and rxdate between dt_rx_index + 1 and dt_rx_index + 180 group by ptid) h on a.ptid=h.ptid
--      left join (select distinct ptid, max(rxdate) as dt_last_bas_bol_ovrlp_bl from ac_ehr_pat_rx_bas_bol_overlap group by ptid) i on a.ptid=i.ptid
group by patid1
order by patid1
;

select * from ac_ehr_sol_16_23_a1c_bl_fu;

-- COMMAND ----------

-- drop table if exists ac_ehr_patient_202308;
-- CREATE TABLE ac_ehr_patient_202308 USING DELTA LOCATION "dbfs:/mnt/optummarkt/202308/ontology/base/Patient";

-- select distinct * from ac_ehr_patient_202308;
select distinct ptid, max(rxdate) as dt_last_sol_bl from ac_ehr_pat_rx_16_23_bas_bol_sol_indx where CATEGORY like ('%iGlarLixi%') and rxdate between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid


-- COMMAND ----------

create or replace table ac_ehr_patient_202308_full_pts as
select distinct a.*,left(FIRST_MONTH_ACTIVE,4) as first_mnth_yr, right(FIRST_MONTH_ACTIVE,2) as first_mnth,
left(LAST_MONTH_ACTIVE,4) as last_mnth_yr, right(LAST_MONTH_ACTIVE,2) as last_mnth from ac_ehr_patient_202308 a;

select distinct * from ac_ehr_patient_202308_full_pts;

create or replace table ac_ehr_patient_202308_full_pts_2 as
select distinct *, cast(concat(first_mnth_yr,'-',first_mnth,'-','01') as date) as first_month_active_new,
cast(concat(last_mnth_yr,'-',last_mnth,'-','01') as date) as last_month_active_new from ac_ehr_patient_202308_full_pts;

select distinct * from ac_ehr_patient_202308_full_pts_2;

-- COMMAND ----------

select distinct birth_yr from ac_ehr_patient_202308_full_pts_2
order by 1

-- COMMAND ----------

drop table if exists ac_ehr_sol_pat_16_23_all_enrol;

create table ac_ehr_sol_pat_16_23_all_enrol as
select distinct a.*,
b.*, c.first_month_active_new , c.last_month_active_new, c.gender, c.birth_yr,year(a.dt_rx_index) as yr_indx, case when c.BIRTH_YR='1934 and Earlier' then 1934 else cast(c.birth_yr as int) end as yrdob
from ac_pat_dx_rx_sol_16_23 a left join ac_ehr_sol_16_23_a1c_bl_fu b on a.ptid=b.patid1
                      left join ac_ehr_patient_202308_full_pts_2 c on a.ptid=c.ptid and a.dt_rx_index between c.first_month_active_new and c.last_month_active_new
order by a.ptid
;

select * from ac_ehr_sol_pat_16_23_all_enrol;

create or replace table ac_ehr_sol_pat_16_23_all_enrol_2 as
select distinct * , yr_indx - yrdob as Age_index from ac_ehr_sol_pat_16_23_all_enrol
order by ptid;

select * from ac_ehr_sol_pat_16_23_all_enrol_2;

-- COMMAND ----------

select * from ac_ehr_sol_pat_all_enrol_2
where ptid='PT386156131'
order by ptid

-- COMMAND ----------

select distinct ptid from ac_ehr_sol_pat_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and dt_1st_t2dm between '2016-07-01' and '2022-06-30' and dt_rx_index between '2017-01-01' and '2021-12-31'

-- COMMAND ----------

drop table if exists ac_ehr_patient_attrition_soliqua_16_23;

create or replace table ac_ehr_patient_attrition_soliqua_16_23 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2016 and 06/30/2022' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where dt_1st_t2dm between '2016-07-01' and '2023-03-31'
union
select ' 2. Had at least one iGlarLixi prescription during the identification (ID) period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where  isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 5a. Had at least one basal insulin prescription during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 5b. (5b) Had at least one bolus insulin prescription during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 6. Had one or more valid HbA1c measurement(s) during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 7. Had one or more valid HbA1c measurement(s) between 90- and 210-days post-index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 7 a. Had one or more iGlarLixi prescription between 90- and 210-days post-index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 8. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 9. Those without any secondary diabetes diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 9b. Had iGlarLixi prescriptions during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 9c. Had basal or bolus prescriptions during the follow-up period (180 days post-index date)' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and isnull(dt_1st_bas_bol_fu) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
-- union
-- select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ac_ehr_patient_attrition_soliqua_16_23 ;

-- COMMAND ----------

drop table if exists ac_ehr_patient_attrition_soliqua_16_23;

create or replace table ac_ehr_patient_attrition_soliqua_16_23 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2016 and 06/30/2022' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where dt_1st_t2dm between '2016-07-01' and '2023-03-31'
union
select ' 2. Had at least one iGlarLixi prescription during the identification (ID) period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where  isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 5a. Had at least one basal insulin prescription during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 5b. (5b) Had at least one bolus insulin prescription during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 6. Had one or more valid HbA1c measurement(s) during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 7. Had one or more valid HbA1c measurement(s) between 90- and 210-days post-index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 7 a. Had one or more iGlarLixi prescription between 90- and 210-days post-index date' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 8. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 9. Those without any secondary diabetes diagnoses identified' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 9b. Had iGlarLixi prescriptions during the baseline period' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
union
select ' 9c. Had basal or bolus prescriptions during the follow-up period (180 days post-index date)' as Step, count(distinct ptid) as n_pat from ac_ehr_sol_pat_16_23_all_enrol_2 where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_bas_bl) and isnotnull(dt_last_bolus_bl) and isnotnull(dt_last_a1c_bl)  and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_sol_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_1st_sec_diab) and isnull(dt_last_sol_bl) and isnull(dt_1st_bas_bol_fu) and dt_1st_t2dm between '2016-07-01' and '2023-03-31' and dt_rx_index between '2017-01-01' and '2022-09-30'
-- union
-- select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct ptid) as n_pat from ac_pat_all_enrol_2 where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and first_month_active_new<=dt_rx_index - 180  and dt_rx_index<=last_month_active_new and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ac_ehr_patient_attrition_soliqua_16_23 ;

-- COMMAND ----------

create or replace  table ac_ehr_rx_anti_dm_optn_2_final as
select ptid, RXDATE, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm, route, QUANTITY_OF_DOSE, STRENGTH, STRENGTH_UNIT,DOSAGE_FORM, DOSE_FREQUENCY, DISCONTINUE_REASON,dt_rx_index, 'WRx' as source from ac_ehr_soliqua_optn_2_rx_wrx
union
select ptid, ADMIN_DATE, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm,ROUTE, QUANTITY_OF_DOSE, STRENGTH, STRENGTH_UNIT, DOSAGE_FORM, DOSE_FREQUENCY, DISCONTINUE_REASON, dt_rx_index, 'Med Admin' as source from ac_ehr_soliqua_optn_med_admin
order by ptid, rxdate
;

select distinct * from ac_ehr_rx_anti_dm_optn_2_final
order by ptid, rxdate;

-- COMMAND ----------


create or replace table ac_ehr_soliqua_optn_2_bl_hba1c as
select distinct a.* from ac_ehr_2305_lab_a1c_sol_index a
inner join ac_ehr_soliqua_attrition_optn_2_final c on a.ptid=c.ptid
where service_date between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1)
order by a.ptid, a.service_date;

select distinct * from ac_ehr_soliqua_optn_2_bl_hba1c
order by ptid, service_date;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_soliqua_optn_2_bl_hba1c

-- COMMAND ----------

select value, count(distinct ptid) as pts from ac_ehr_soliqua_optn_2_bl_hba1c
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_soliqua_optn_2_bl_hba1c_max as
select distinct ptid, max(value) as max_value from ac_ehr_soliqua_optn_2_bl_hba1c
group by 1
order by 1;

select distinct * from ac_ehr_soliqua_optn_2_bl_hba1c_max
order by 1;

-- COMMAND ----------

select max_value, count(distinct ptid) as pts from ac_ehr_soliqua_optn_2_bl_hba1c_max
group by 1
order by 1;
