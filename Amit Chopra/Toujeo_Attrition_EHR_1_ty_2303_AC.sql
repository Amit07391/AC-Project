-- Databricks notebook source
------------include Observation data-------
drop table if exists ty51_mc_2303_Observation;

CREATE TABLE ty51_mc_2303_Observation USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/Observation';

select * from ty51_mc_2303_Observation;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(obs_date) as dt_obs_start, max(obs_date) as dt_obs_stop
from ty51_mc_2303_Observation;

-- COMMAND ----------

------------include Diagnosis data-------
drop table if exists ty51_mc_2303_Diagnosis;

CREATE TABLE ty51_mc_2303_Diagnosis USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/Diagnosis';

select * from ty51_mc_2303_Diagnosis;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(diag_date) as dt_dx_start, max(diag_date) as dt_dx_stop
from ty51_mc_2303_Diagnosis;



-- COMMAND ----------

drop table if exists ty51_mc_2303_RX_Prescribed;

CREATE TABLE ty51_mc_2303_RX_Prescribed USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/RX Prescribed';

select * from ty51_mc_2303_RX_Prescribed;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(rxdate) as dt_rx_start, max(rxdate) as dt_rx_stop
from ty51_mc_2303_RX_Prescribed;


-- COMMAND ----------

------------include RX Administration data-------
drop table if exists ty51_mc_2303_RX_Administration;

CREATE TABLE ty51_mc_2303_RX_Administration USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/RX Administration';

select * from ty51_mc_2303_RX_Administration;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(admin_date) as dt_rx_start, max(admin_date) as dt_rx_stop
from ty51_mc_2303_RX_Administration;


-- COMMAND ----------

------------include Procedure data-------
drop table if exists ty51_mc_2303_Procedure;

CREATE TABLE ty51_mc_2303_Procedure USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/Procedure';

select * from ty51_mc_2303_Procedure;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(proc_date) as dt_pr_start, max(proc_date) as dt_pr_stop
from ty51_mc_2303_Procedure;

-- COMMAND ----------

drop table if exists ty51_mc_2303_Lab;

CREATE TABLE ty51_mc_2303_Lab USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/Lab';

select * from ty51_mc_2303_Lab;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(result_date) as dt_lab_start, max(result_date) as dt_lab_stop
from ty51_mc_2303_Lab;

-- COMMAND ----------

------------include Patient data-------
drop table if exists ty51_mc_2303_Patient;

CREATE TABLE ty51_mc_2303_Patient USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/Patient';

select * from ty51_mc_2303_Patient;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(first_month_active) as dt_act_start, max(last_month_active) as dt_act_stop
from ty51_mc_2303_Patient;


-- COMMAND ----------

DROP TABLE IF EXISTS ty51_mc_2303_Patient_active;

create table ty51_mc_2303_Patient_active as
select *, case when isnotnull(DATE_OF_DEATH) then last_day(cast(concat_ws('-',substr(DATE_OF_DEATH,1,4),substr(DATE_OF_DEATH,5,2),'01') as date))
               else null end as dt_death
        , case when isnotnull(FIRST_MONTH_ACTIVE) then cast(concat_ws('-',substr(FIRST_MONTH_ACTIVE,1,4),substr(FIRST_MONTH_ACTIVE,5,2),'01') as date)
               else null end as dt_1st_active
        , case when isnotnull(LAST_MONTH_ACTIVE) and isnotnull(DATE_OF_DEATH) and DATE_OF_DEATH<=LAST_MONTH_ACTIVE then last_day(cast(concat_ws('-',substr(DATE_OF_DEATH,1,4),substr(DATE_OF_DEATH,5,2),'01') as date))
               when isnotnull(LAST_MONTH_ACTIVE) and isnotnull(DATE_OF_DEATH) and DATE_OF_DEATH>LAST_MONTH_ACTIVE then last_day(cast(concat_ws('-',substr(LAST_MONTH_ACTIVE,1,4),substr(LAST_MONTH_ACTIVE,5,2),'01') as date))
               when isnotnull(LAST_MONTH_ACTIVE) and isnull(DATE_OF_DEATH) then last_day(cast(concat_ws('-',substr(LAST_MONTH_ACTIVE,1,4),substr(LAST_MONTH_ACTIVE,5,2),'01') as date))
               else null end as dt_last_active
from ty51_mc_2303_Patient
order by ptid
;

select * from ty51_mc_2303_Patient_active;

-- COMMAND ----------

------------include NLP SDS Family data-------
drop table if exists ty51_mc_2303_sds_family;

CREATE TABLE ty51_mc_2303_sds_family USING DELTA LOCATION 'dbfs:/mnt/optummarkt/202303/ontology/base/NLP SDS Family';

select * from ty51_mc_2303_sds_family;

drop table if exists ty51_mc_2303_sds_family_normal;

create table ty51_mc_2303_sds_family_normal as
select *,
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
from ty51_mc_2303_sds_family
order by ptid
;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(note_date) as dt_sds_start, max(note_date) as dt_sds_stop
from ty51_mc_2303_sds_family_normal;
--Version 202303: N obs=18,238,276,868; N pat=73,946,795 (2007-01-01, 2022-06-30)   1.16 hours runtime



-- COMMAND ----------

drop table if exists ty52_pat_dx_dm;

create table ty52_pat_dx_dm as
select distinct *,
    case when diagnosis_cd like 'E11%' or (diagnosis_cd like '250%' and substr(diagnosis_cd,5,1) in ('0','2')) then 'T2DM'
         else 'T1DM' end as dx_name
from ty51_mc_2303_Diagnosis
where (diagnosis_cd like 'E11%' or (diagnosis_cd like '250%' and substr(diagnosis_cd,5,1) in ('0','2'))
   or diagnosis_cd like 'E10%' or (diagnosis_cd like '250%' and substr(diagnosis_cd,5,1) in ('1','3'))
   or diagnosis_cd like 'E08%' or diagnosis_cd like 'E09%' or (diagnosis_cd like '249%'))
   and DIAGNOSIS_STATUS='Diagnosis of'
;

select * from ty52_pat_dx_dm;


-- COMMAND ----------

select dx_name, count(*) as n_obs, count(distinct ptid) as n_pat, min(diag_date) as dt_dm_start, max(diag_date) as dt_dm_stop
from ty52_pat_dx_dm
group by dx_name
;


-- COMMAND ----------

drop table if exists ty52_rx_pres_anti_dm;

create table ty52_rx_pres_anti_dm as
select distinct a.ptid, a.rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.daily_dose, a.dose_frequency, a.quantity_per_fill
, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, b.*
from ty51_mc_2303_RX_Prescribed a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, a.rxdate
;

select * from ty52_rx_pres_anti_dm;


-- COMMAND ----------

drop table if exists ty52_rx_admi_anti_dm;

create table ty52_rx_admi_anti_dm as
select distinct a.ptid, a.admin_date as rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.dose_frequency
, a.generic_desc, a.drug_class, b.*
from ty51_mc_2303_RX_Administration a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, rxdate
;

select * from ty52_rx_admi_anti_dm;


-- COMMAND ----------

drop table if exists ty52_rx_anti_dm;

create table ty52_rx_anti_dm as
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm, 'Pres' as source from ty52_rx_pres_anti_dm
union
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm, 'Admi' as source from ty52_rx_admi_anti_dm
order by ptid, rxdate
;

select * from ty52_rx_anti_dm;

select rx_type, drug_name
from ty52_rx_anti_dm
group by rx_type, drug_name
order by rx_type, drug_name
;


-- COMMAND ----------

drop table if exists ty52_pat_rx_basal;

create table ty52_pat_rx_basal as
select distinct ptid, rxdate,drug_name,generic_desc,ndc,rx_type,gnrc_nm,brnd_nm
       , case when lcase(drug_name) like '%toujeo%' then 'Toujeo'
              when lcase(rx_type) like '%basal%' and lcase(drug_name) not like '%toujeo%' then 'Other long-acting BIs'
              when lcase(rx_type) like '%glp1%' then 'GLP1'
                   else rx_type end as rx_type2
from ty52_rx_anti_dm
where rxdate>='2014-07-01' and (lcase(rx_type) in ('basal', 'bolus', 'premix')
        or (lcase(brnd_nm) in ('trulicity','ozempic','bydureon','bydureon bcise','bydureon pen')))
order by ptid, rxdate
;

select rx_type2, rx_type, gnrc_nm,brnd_nm, min(rxdate) as dt_rx_start, max(rxdate) as dt_rx_end
from ty52_pat_rx_basal
group by rx_type2, rx_type, gnrc_nm,brnd_nm
order by rx_type2, rx_type, gnrc_nm,brnd_nm
;


-- COMMAND ----------

drop table if exists ty52_pat_dx_rx;

create table ty52_pat_dx_rx as
select distinct a.ptid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_toujeo) as dt_1st_toujeo
        , min(d.dt_1st_other_bi) as dt_rx_other_insulin
        , case when isnotnull(min(c.dt_1st_toujeo)) then min(c.dt_1st_toujeo)
               else min(d.dt_1st_other_bi) end as dt_rx_index
        , case when isnotnull(min(c.dt_1st_toujeo)) then 'Toujeo'
               when isnotnull(min(d.dt_1st_other_bi)) then 'Other long-acting BIs'
               else null end as index_group
from (select distinct ptid, min(diag_date) as dt_1st_t2dm, count(distinct diag_date) as n_t2dm from ty52_pat_dx_dm where dx_name='T2DM' group by ptid) a
      left join (select distinct ptid, min(diag_date) as dt_1st_t1dm, count(distinct diag_date) as n_t1dm from ty52_pat_dx_dm where dx_name='T1DM' group by ptid) b on a.ptid=b.ptid
      left join (select distinct ptid, min(rxdate) as dt_1st_toujeo from ty52_pat_rx_basal where rxdate>='2015-01-01' and rx_type2 in ('Toujeo') group by ptid) c on a.ptid=c.ptid
      left join (select distinct ptid, min(rxdate) as dt_1st_other_bi from ty52_pat_rx_basal where rxdate>='2015-01-01' and rx_type2 in ('Other long-acting BIs') group by ptid) d on a.ptid=d.ptid
group by a.ptid
order by a.ptid
;

select count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_t2dm) as dt_d2dm_start, max(dt_1st_t2dm) as dt_d2dm_end
from ty52_pat_dx_rx
where isnotnull(dt_rx_index)
;


-- COMMAND ----------

select index_group, count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_rx_index) as dt_rx_start, max(dt_rx_index) as dt_rx_end
from ty52_pat_dx_rx
group by index_group
;

-- COMMAND ----------

drop table if exists ty52_pat_rx_basal_index;

create table ty52_pat_rx_basal_index as
select distinct a.*, b.dt_rx_index
from ty52_pat_rx_basal a left join ty52_pat_dx_rx b
on a.ptid=b.ptid
order by a.ptid, a.rxdate
;

select * from ty52_pat_rx_basal_index;

-- COMMAND ----------

select min(rxdate) from ty52_pat_rx_basal_index

-- COMMAND ----------

drop table if exists ty52_lab_antibody_name;

create table ty52_lab_antibody_name as
select distinct *, cast(test_result as double) as result
, case when isnotnull(result_date) then result_date
       when isnotnull(collected_date) then collected_date
       when isnotnull(order_date) then order_date
       else null end as test_date
from ty51_mc_2303_Lab
where lcase(test_name) in ('islet antigen 2 antibody (ia-2a)', 'pancreatic islet cell antibody (ica)', 'zinc transporter 8 antibody (znt8)'
, 'glutamic acid decarboxylase-65 antibody (gad65)', 'insulin antibody', 'hemoglobin a1c', 'glucose.fasting','glucose.tolerance test.1 hour','glucose.tolerance test.2 hour','glucose.tolerance test.3 hour')
order by ptid, test_date
;

select * from ty52_lab_antibody_name;

-- COMMAND ----------

drop table if exists ty52_lab_antibody_name_value;

create table ty52_lab_antibody_name_value as
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
from ty52_lab_antibody_name
;

select count(*) as n_obs
from ty52_lab_antibody_name_value
where isnull(result) and isnotnull(value)
;


-- COMMAND ----------

drop table if exists ty52_lab_a1c_index;

create table ty52_lab_a1c_index as
select a.*, b.dt_rx_index
from ty52_lab_antibody_name_value a join ty52_pat_dx_rx b
on a.ptid=b.ptid
where isnotnull(b.dt_rx_index) and a.value between 3 and 15 and lcase(a.test_name) in ('hemoglobin a1c')
order by a.ptid, a.collected_date
;

select * from ty52_lab_a1c_index;


-- COMMAND ----------

drop table if exists ty52_glp1_a1c_bl_fu;

create table ty52_glp1_a1c_bl_fu as
select distinct a.ptid as ptid1, max(b.dt_last_glp1_bl) as dt_last_glp1_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, min(e.dt_1st_glp1_fu) as dt_1st_glp1_fu, max(f.dt_last_insulin_bl) as dt_last_insulin_bl
from (select ptid, dt_rx_index from ty52_pat_dx_rx where isnotnull(dt_rx_index)) a
     left join (select distinct ptid, max(rxdate) as dt_last_glp1_bl from ty52_pat_rx_basal_index where lcase(rx_type) in ('glp1') and rxdate between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1) group by ptid) b on a.ptid=b.ptid
     left join (select distinct ptid, max(collected_date) as dt_last_a1c_bl from ty52_lab_a1c_index where collected_date between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) c on a.ptid=c.ptid
     left join (select distinct ptid, min(collected_date) as dt_1st_a1c_fu from ty52_lab_a1c_index where collected_date between date_add(dt_rx_index,90) and date_add(dt_rx_index,210) group by ptid) d on a.ptid=d.ptid
     left join (select distinct ptid, min(rxdate) as dt_1st_glp1_fu from ty52_pat_rx_basal_index where lcase(rx_type) in ('glp1') and rxdate >= dt_rx_index group by ptid) e on a.ptid=e.ptid
     left join (select distinct ptid, max(rxdate) as dt_last_insulin_bl from ty52_pat_rx_basal_index where lcase(rx_type) not in ('glp1') and rxdate between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by ptid) f on a.ptid=f.ptid
group by ptid1
order by ptid1
;

select * from ty52_glp1_a1c_bl_fu;


-- COMMAND ----------

drop table if exists ty52_pat_all_enrol;

create table ty52_pat_all_enrol as
select distinct a.*, case when index_group='Toujeo' then index_group
                          when index_group='Other long-acting BIs' then 'Other long-acting BIs'
                          else null end as index_group2
                   , case when index_group='Toujeo' then dt_rx_index
                          when index_group='Other long-acting BIs' then dt_rx_index
                          else null end as dt_rx_index2
                   , b.*, c.dt_1st_active as enrlstdt, c.dt_last_active as enrlendt, c.gender, c.birth_yr, year(a.dt_rx_index)-c.birth_yr as age_index
                   , c.race, c.region, c.division, c.date_of_death, c.first_month_active, c.last_month_active
from ty52_pat_dx_rx a left join ty52_glp1_a1c_bl_fu b on a.ptid=b.ptid1
                      left join ty51_mc_2303_Patient_active c on a.ptid=c.ptid and a.dt_rx_index between c.dt_1st_active and c.dt_last_active
order by a.ptid
;

select * from ty52_pat_all_enrol;


-- COMMAND ----------

drop table if exists ty52_patient_attrition;

create table ty52_patient_attrition as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 3/31/2022' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty52_patient_attrition;

drop table if exists ty52_patient_attrition_pct;

create table ty52_patient_attrition_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty52_patient_attrition)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty52_patient_attrition_pct
;


-- COMMAND ----------

drop table if exists ty52_patient_attrition_toujeo;

create table ty52_patient_attrition_toujeo as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty52_patient_attrition_toujeo;

drop table if exists ty52_patient_attrition_toujeo_pct;

create table ty52_patient_attrition_toujeo_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty52_patient_attrition_toujeo)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty52_patient_attrition_toujeo_pct
;



-- COMMAND ----------

drop table if exists ty52_patient_attrition_other;

create table ty52_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct ptid) as n_pat from ty52_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty52_patient_attrition_other;

drop table if exists ty52_patient_attrition_other_pct;

create table ty52_patient_attrition_other_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty52_patient_attrition_other)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty52_patient_attrition_other_pct
;


