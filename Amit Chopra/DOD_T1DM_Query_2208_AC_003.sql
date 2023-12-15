-- Databricks notebook source

--Codes for creating ty37_lab_a1c_loinc_value:

drop table if exists ty37_lab_a1c_loinc; 
create table ty37_lab_a1c_loinc as
select distinct *, cast(rslt_txt as double) as result
from ty37_dod_2208_lab_result
where lcase(loinc_cd) in ('17855-8', '17856-6','41995-2','4548-4','45484','4637-5','55454-3','hgba1c') or
     (isnull(loinc_cd) and lcase(tst_desc) in ('a1c','glyco hemoglobin a1c','glycohemoglobin (a1c) glycohem','glycohemoglobin a1c','hemoglobin a1c','hgb a1c','hba1c','hemoglob a1c','hemoglobin a1c'
,'hemoglobin a1c w/o eag','hgb-a1c','hgba1c-t'))
order by patid, fst_dt
; select * from ty37_lab_a1c_loinc;
select loinc_cd, tst_desc, count(*) as n_obs from ty37_lab_a1c_loinc
--where lcase(tst_desc) like '%a1c%'
group by loinc_cd, tst_desc
order by loinc_cd, tst_desc
; select RSLT_TXT, count(*) as n_obs from ty37_lab_a1c_loinc
--where lcase(tst_desc) like '%a1c%'
group by RSLT_TXT
order by RSLT_TXT
; drop table if exists ty37_lab_a1c_loinc_value; create table ty37_lab_a1c_loinc_value as
select *, case when rslt_nbr>0 then rslt_nbr
               when rslt_nbr=0 and isnotnull(result) then result
               when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%' or rslt_txt like '%=%' or substr(rslt_txt,-1)='%') then cast(rslt_txt as double)
               when rslt_nbr=0 and isnull(result) and rslt_txt like '>%' and not(rslt_txt like '>=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)+0.1
               when rslt_nbr=0 and isnull(result) and rslt_txt like '>=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)+0.1
               when rslt_nbr=0 and isnull(result) and rslt_txt like '<%' and not(rslt_txt like '<=%') and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,2) as double)-0.1
               when rslt_nbr=0 and isnull(result) and rslt_txt like '<=%' and not(substr(rslt_txt,-1)='%') then cast(substr(rslt_txt,3) as double)-0.1
               when rslt_nbr=0 and isnull(result) and not(rslt_txt like '%>%' or rslt_txt like '%<%') and substr(rslt_txt,-1)='%' then cast(substring_index(rslt_txt,'%',1) as double)
               else null end value
from ty37_lab_a1c_loinc
; select count(*) as n_obs
from ty37_lab_a1c_loinc_value
where rslt_nbr=0 and isnotnull(value)
;


Codes for creating ty37_rx_anti_dm:


drop table if exists ty19_rx_anti_dm; create table ty37_rx_anti_dm as
select distinct a.PATID, a.PAT_PLANID, a.AVGWHLSL, a.CHARGE, a.CLMID, a.COPAY, a.DAW, a.DAYS_SUP, a.DEDUCT, a.DISPFEE, a.FILL_DT, a.MAIL_IND, a.NPI, a.PRC_TYP, a.QUANTITY
        , a.RFL_NBR, a.SPECCLSS, a.STD_COST, a.STD_COST_YR, a.STRENGTH, b.*
from ty37_dod_2208_rx_claim a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
; select * from ty37_rx_anti_dm; select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(fill_dt) as dt_rx_start, max(fill_dt) as dt_rx_stop
from ty37_rx_anti_dm;


