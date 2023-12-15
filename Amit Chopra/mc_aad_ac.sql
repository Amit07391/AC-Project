-- Databricks notebook source
----------Import MarketClick diagnosis records---------
drop table if exists ty18_2205_med_diag;

create table ty18_2205_med_diag using delta location 'dbfs:/mnt/optummarkt/202205/ontology/base/Diagnosis';

select * from ty18_2205_med_diag;

-- COMMAND ----------

----------Create Diag code for the project---------

drop table if exists ty18_study_dx_codes;

create table ty18_study_dx_codes as
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_CHADVASC_codelist
union
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_codelist_v2
union
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_comorbidities_codelist where DOMAIN='Diagnosis'
union
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_exclusion_criteria_codelist_v2 where DOMAIN='Diagnosis'
order by COHORT_NAME, CONDITION, SUB_CONDITION, CODE
;

select COHORT_NAME, CONDITION, SUB_CONDITION, count(*) as n_code
from ty18_study_dx_codes
group by COHORT_NAME, CONDITION, SUB_CONDITION
order by COHORT_NAME, CONDITION, SUB_CONDITION
;

-- COMMAND ----------

drop table if exists ty18_med_diag_subset;

create table ty18_med_diag_subset as
select distinct a.ptid, a.encid, a.diag_date, a.diagnosis_cd, a.diagnosis_cd_type, a.diagnosis_status, a.poa, a.admitting_diagnosis, a.discharge_diagnosis, a.primary_diagnosis
        , year(a.diag_date) as year, b.*
from ty18_2205_med_diag a join ty18_study_dx_codes b
on a.diagnosis_cd=b.code and a.diagnosis_status='Diagnosis of'
order by a.ptid, a.diag_date, a.encid
;

select * from ty18_med_diag_subset;


-- COMMAND ----------

select CONDITION, sub_CONDITION, format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(diag_date) as dt_dx_start, max(diag_date) as dt_dx_stop
from ty18_med_diag_subset
group by CONDITION, sub_CONDITION
order by CONDITION, sub_CONDITION
;

-- COMMAND ----------

drop table if exists ty18_dx_any_af_index;

create table ty18_dx_any_af_index as
select distinct ptid, year, diag_date as dt_1st_dx_af, encid, condition, sub_condition, rank
from (select *, dense_rank() over (partition by ptid order by diag_date) as rank
      from ty18_med_diag_subset where condition='Atrial Fibrillation' and isnotnull(diag_date))
where rank<=1
order by ptid
;

select * from ty18_dx_any_af_index;

-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(dt_1st_dx_af) as dt_rx_start, max(dt_1st_dx_af) as dt_rx_end
from ty18_dx_any_af_index
where dt_1st_dx_af between '2017-01-01' and '2021-12-31'
;

-- COMMAND ----------

select a.*, b.n_encid
from ty18_dx_any_af_index a join (select ptid, count(*) as n_encid from ty18_dx_any_af_index group by ptid) b
on a.ptid=b.ptid and b.n_encid>1
order by a.ptid
;


-- COMMAND ----------

----------Import MarketClick Encounter records---------
drop table if exists ty18_2205_encounter;

create table ty18_2205_encounter using delta location 'dbfs:/mnt/optummarkt/202205/ontology/base/Encounter';

select * from ty18_2205_encounter;

-- COMMAND ----------

----------Import MarketClick Visit records---------
drop table if exists ty18_2205_visit;

create table ty18_2205_visit using delta location 'dbfs:/mnt/optummarkt/202205/ontology/base/Visit';

select * from ty18_2205_visit;

-- COMMAND ----------

drop table if exists ty18_dx_any_af_index_inp;

create table ty18_dx_any_af_index_inp as
select distinct a.*, b.interaction_type, b.visitid, b.interaction_date, c.visit_type, c.visit_start_date, c.visit_end_date, c.discharge_disposition, c.admission_source, c.drg, c.fl_inp
from ty18_dx_any_af_index a left join (select * from ty18_2205_encounter where interaction_type='Inpatient') b on a.ptid=b.ptid and a.encid=b.encid
                            left join (select *, 1 as fl_inp from ty18_2205_visit where visit_type='Inpatient' and isnotnull(visit_start_date)) c on a.ptid=c.ptid and b.visitid=c.visitid
order by a.ptid
;

select * from ty18_dx_any_af_index_inp;


-- COMMAND ----------

select a.*, b.n_encid
from ty18_dx_any_af_index_inp a join (select ptid, encid, count(*) as n_encid from ty18_dx_any_af_index_inp where visit_type='Inpatient' group by ptid, encid) b
on a.ptid=b.ptid and a.encid=b.encid and b.n_encid>1
order by a.ptid
;


-- COMMAND ----------

drop table if exists ty18_dx_any_af_index_inp_final;

create table ty18_dx_any_af_index_inp_final as
select distinct *
from (select *, dense_rank() over (partition by ptid order by visit_end_date desc, sub_condition desc) as rank2
      from ty18_dx_any_af_index_inp)
where rank2<=1
order by ptid
;

select format_number(count(*),0) as n_obs, format_number(count(distinct ptid),0) as n_pat, min(dt_1st_dx_af) as dt_dx_start, max(dt_1st_dx_af) as dt_dx_end
from ty18_dx_any_af_index_inp_final
where dt_1st_dx_af between '2017-01-01' and '2021-12-31'
;

-- COMMAND ----------

select a.*, b.n_encid
from ty18_dx_any_af_index_inp_final a join (select ptid, count(*) as n_encid from ty18_dx_any_af_index_inp_final group by ptid) b
on a.ptid=b.ptid and b.n_encid>1
order by a.ptid
;


-- COMMAND ----------

----------Import MarketClick Rx Administration records---------
drop table if exists ty18_2205_rx_admin;

create table ty18_2205_rx_admin using delta location 'dbfs:/mnt/optummarkt/202205/ontology/base/RX Administration';

select * from ty18_2205_rx_admin;

-- COMMAND ----------

----------Import MarketClick Rx Prescribed records---------
drop table if exists ty18_2205_rx_presc;

create table ty18_2205_rx_presc using delta location 'dbfs:/mnt/optummarkt/202205/ontology/base/RX Prescribed';

select * from ty18_2205_rx_presc;


-- COMMAND ----------

----------Create Rx (NDC) code for the project---------

drop table if exists ty18_study_rx_codes;

create table ty18_study_rx_codes as
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_medication_codelist
union
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_treatment_aad_codelist where DOMAIN='Drug'
union
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_exclusion_criteria_codelist_v2 where DOMAIN='Drug'
union
select COHORT_NAME, CONDITION, SUB_CONDITION, CODE, DESCRIPTION, VOCABULARY from ty13_cohort_comorbidities_codelist where DOMAIN='Drug'
order by COHORT_NAME, CONDITION, SUB_CONDITION, CODE
;

select COHORT_NAME, CONDITION, SUB_CONDITION, count(*) as n_code
from ty18_study_rx_codes
group by COHORT_NAME, CONDITION, SUB_CONDITION
order by COHORT_NAME, CONDITION, SUB_CONDITION
;


-- COMMAND ----------

select * from ty18_study_rx_codes;


-- COMMAND ----------

select * from ty18_study_rx_codes;

drop table if exists ty18_rx_admin_subset;

create table ty18_rx_admin_subset as
select distinct a.ptid, a.encid, a.drug_name, a.ndc, a.ndc_source, a.order_date, a.admin_date, a.provid, a.route, a.quantity_of_dose, a.strength
, a.strength_unit, a.dosage_form, a.dose_frequency, a.generic_desc, a.drug_class, a.discontinue_reason, year(a.admin_date) as year, b.*
from ty18_2205_rx_admin a join ty18_study_rx_codes b
on a.ndc=b.code
order by a.ptid, a.admin_date, a.encid
;

select * from ty18_rx_admin_subset;


-- COMMAND ----------

drop table if exists ty18_rx_presc_subset;

create table ty18_rx_presc_subset as
select distinct a.ptid, a.rxdate, a.drug_name, a.ndc, a.ndc_source, a.provid, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form
, a.daily_dose, a.dose_frequency, a.quantity_per_fill, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, a.discontinue_reason, year(a.rxdate) as year, b.*
from ty18_2205_rx_presc a join ty18_study_rx_codes b
on a.ndc=b.code
order by a.ptid, a.rxdate
;

select * from ty18_rx_presc_subset;


-- COMMAND ----------

drop table if exists ty18_aad_rx_admin;

create table ty18_aad_rx_admin as
select distinct ptid, encid, year, admin_date as dt_1st_aad_inp, sub_CONDITION, rank
from (select *, dense_rank() over (partition by ptid order by admin_date) as rank
      from ty18_rx_admin_subset where lcase(sub_CONDITION) in ('amiodarone','dofetilide','dronedarone','flecainide','propafenone','sotalol') and isnotnull(admin_date))
where rank<=1
order by ptid
;

select * from ty18_aad_rx_admin;

-- COMMAND ----------

select count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_aad_inp) as dt_rx_start, max(dt_1st_aad_inp) as dt_rx_end
from ty18_aad_rx_admin
where dt_1st_aad_inp between '2017-01-01' and '2021-12-31'
;


-- COMMAND ----------

select a.*, b.n_obs
from ty18_aad_rx_admin a join (select ptid, count(*) as n_obs from ty18_aad_rx_admin group by ptid) b
on a.ptid=b.ptid and b.n_obs>1
order by a.ptid
;


-- COMMAND ----------

drop table if exists ty18_aad_rx_presc;

create table ty18_aad_rx_presc as
select distinct ptid, year, rxdate as dt_1st_aad_out, sub_CONDITION, rank
from (select *, dense_rank() over (partition by ptid order by rxdate) as rank
      from ty18_rx_presc_subset where lcase(sub_CONDITION) in ('amiodarone','dofetilide','dronedarone','flecainide','propafenone','sotalol') and isnotnull(rxdate))
where rank<=1
order by ptid
;

select count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_aad_out) as dt_rx_start, max(dt_1st_aad_out) as dt_rx_end
from ty18_aad_rx_presc
where dt_1st_aad_out between '2017-01-01' and '2021-12-31'
;


-- COMMAND ----------

select a.*, b.n_obs
from ty18_aad_rx_presc a join (select ptid, count(*) as n_obs from ty18_aad_rx_presc group by ptid) b
on a.ptid=b.ptid and b.n_obs>1
order by a.ptid
;


-- COMMAND ----------

drop table if exists ty18_aad_rx;

create table ty18_aad_rx as
select distinct coalesce(a.ptid, b.ptid) as ptid, coalesce(a.year, b.year) as year, coalesce(a.sub_CONDITION, b.sub_CONDITION) as sub_CONDITION, a.dt_1st_aad_inp, a.encid, b.dt_1st_aad_out
, case when isnotnull(a.dt_1st_aad_inp) and isnull(b.dt_1st_aad_out) then a.dt_1st_aad_inp
       when isnull(a.dt_1st_aad_inp) and isnotnull(b.dt_1st_aad_out) then b.dt_1st_aad_out
       when isnotnull(a.dt_1st_aad_inp) and isnotnull(b.dt_1st_aad_out) and a.dt_1st_aad_inp<=b.dt_1st_aad_out then a.dt_1st_aad_inp
       when isnotnull(a.dt_1st_aad_inp) and isnotnull(b.dt_1st_aad_out) and a.dt_1st_aad_inp>b.dt_1st_aad_out then b.dt_1st_aad_out
       else null end dt_1st_rx_aad
, case when isnotnull(a.dt_1st_aad_inp) and isnull(b.dt_1st_aad_out) then 'Inp'
       when isnull(a.dt_1st_aad_inp) and isnotnull(b.dt_1st_aad_out) then 'Out'
       when isnotnull(a.dt_1st_aad_inp) and isnotnull(b.dt_1st_aad_out) and a.dt_1st_aad_inp<=b.dt_1st_aad_out then 'Inp'
       when isnotnull(a.dt_1st_aad_inp) and isnotnull(b.dt_1st_aad_out) and a.dt_1st_aad_inp>b.dt_1st_aad_out then 'Out'
       else null end source
from ty18_aad_rx_admin a full join ty18_aad_rx_presc b
on a.ptid=b.ptid and a.year=b.year and a.sub_CONDITION=b.sub_CONDITION
order by ptid
;

select year, sub_CONDITION, source, count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_rx_aad) as dt_rx_start, max(dt_1st_rx_aad) as dt_rx_end
from ty18_aad_rx
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
group by year, sub_CONDITION, source
order by year, sub_CONDITION, source
;


-- COMMAND ----------

drop table if exists ty18_aad_rx_af_dx_full;

create table ty18_aad_rx_af_dx_full as
select distinct coalesce(a.ptid, b.ptid) as ptid, a.year as year_rx, a.sub_CONDITION, a.dt_1st_rx_aad, a.source
       , b.year as year_dx, b.dt_1st_dx_af, b.encid, b.interaction_type, b.visitid, b.visit_start_date, b.visit_end_date
       , case when a.source='Inp' then 'Inp_admin_rx'
              when a.source='Out' and isnotnull(a.dt_1st_rx_aad) and a.dt_1st_rx_aad between b.visit_start_date and date_sub(b.visit_end_date,1) then 'Inp_out_rx'
              when a.source='Out' and isnotnull(a.dt_1st_rx_aad) and not(a.dt_1st_rx_aad between b.visit_start_date and date_sub(b.visit_end_date,1)) then 'Out_rx'
              when a.source='Out' and isnotnull(a.dt_1st_rx_aad) and isnull(b.visit_start_date) then 'Out_rx'
              else null end as source_rx
       , case when a.source='Inp' then 'Inp'
              when a.source='Out' and isnotnull(a.dt_1st_rx_aad) and a.dt_1st_rx_aad between b.visit_start_date and date_sub(b.visit_end_date,1) then 'Inp'
              when a.source='Out' and isnotnull(a.dt_1st_rx_aad) and not(a.dt_1st_rx_aad between b.visit_start_date and date_sub(b.visit_end_date,1)) then 'Out'
              when a.source='Out' and isnotnull(a.dt_1st_rx_aad) and isnull(b.visit_start_date) then 'Out'
              else null end as source_rx2
       , case when isnotnull(b.interaction_type) then 'Inp'
              else 'Out' end as source_af
       , case when isnotnull(a.dt_1st_rx_aad) then 'Yes'
              else 'No' end as aad_used
from ty18_aad_rx a full join ty18_dx_any_af_index_inp_final b
on a.ptid=b.ptid and b.year=a.year
order by ptid
;

select * from ty18_aad_rx_af_dx_full;


-- COMMAND ----------

select year_dx, source_rx2, source_rx, count(distinct ptid) as n_pat, min(dt_1st_dx_af) as dt_rx_start, max(dt_1st_dx_af) as dt_rx_stop
from ty18_aad_rx_af_dx_full
where dt_1st_dx_af between '2017-01-01' and '2021-12-31'
group by year_dx, source_rx2, source_rx
order by year_dx, source_rx2, source_rx
;


-- COMMAND ----------

select year_dx, source_rx2, source_rx, count(distinct ptid) as n_pat, min(dt_1st_dx_af) as dt_rx_start, max(dt_1st_dx_af) as dt_rx_stop
from ty18_aad_rx_af_dx_full
where dt_1st_dx_af between '2017-01-01' and '2021-12-31'
group by year_dx, source_rx2, source_rx
order by year_dx, source_rx2, source_rx
;

-- COMMAND ----------

select year_dx, source_rx, count(distinct ptid) as n_pat, min(dt_1st_dx_af) as dt_rx_start, max(dt_1st_dx_af) as dt_rx_stop
from ty18_aad_rx_af_dx_full
where dt_1st_dx_af between '2017-01-01' and '2021-12-31'
group by year_dx, source_rx
order by year_dx, source_rx
;


-- COMMAND ----------

select *
from ty18_aad_rx_af_dx_full
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31' and source_rx='Inp_out_rx'
--group by year_rx, source_rx
order by ptid
;

-- COMMAND ----------

drop table if exists ty18_any_aad_n_17_21;

create table ty18_any_aad_n_17_21 as
select '*2017-2021' as period, 'any AAD' as drug, source_af, aad_used, source_rx2, count(distinct ptid) as n_pat
from ty18_aad_rx_af_dx_full
where year_dx between 2017 and 2021
group by source_af, aad_used, source_rx2
order by source_af, aad_used, source_rx2
;

select * from ty18_any_aad_n_17_21;

-- COMMAND ----------

describe table ty18_any_aad_n_17_21;

-- COMMAND ----------

drop table if exists ty18_any_aad_n_year;

create table ty18_any_aad_n_year as
select cast(year_dx as string) as period, 'any AAD' as drug, source_af, aad_used, source_rx2, count(distinct ptid) as n_pat
from ty18_aad_rx_af_dx_full
where year_dx between 2017 and 2021
group by period, source_af, aad_used, source_rx2
order by period, drug, source_af, aad_used, source_rx2
;

select * from ty18_any_aad_n_year;

-- COMMAND ----------

drop table if exists ty18_each_aad_n_17_21;

create table ty18_each_aad_n_17_21 as
select '*2017-2021' as period, sub_CONDITION as drug, source_af, aad_used, source_rx2, count(distinct ptid) as n_pat
from ty18_aad_rx_af_dx_full
where year_dx between 2017 and 2021 
--and aad_used='Yes'
group by period, drug, source_af, aad_used, source_rx2
order by period, drug, source_af, aad_used, source_rx2
;

select * from ty18_each_aad_n_17_21;

-- COMMAND ----------

drop table if exists ty18_each_aad_n_year;

create table ty18_each_aad_n_year as
select cast(year_dx as string) as period, sub_CONDITION as drug, source_af, aad_used, source_rx2, count(distinct ptid) as n_pat
from ty18_aad_rx_af_dx_full
where year_dx between 2017 and 2021 
--and aad_used='Yes'
group by period, drug, source_af, aad_used, source_rx2
order by period, drug, source_af, aad_used, source_rx2
;

select * from ty18_each_aad_n_year;

-- COMMAND ----------

drop table if exists ty18_aad_n_all;

create table ty18_aad_n_all as
select * from ty18_any_aad_n_17_21
union
select * from ty18_any_aad_n_year
union
select * from ty18_each_aad_n_17_21
union
select * from ty18_each_aad_n_year
order by period, drug, source_af, aad_used, source_rx2
;

select * from ty18_aad_n_all;


-- COMMAND ----------

----------Import MarketClarity Procedure records---------
drop table if exists ty18_2205_Procedure;

create table ty18_2205_Procedure using delta location 'dbfs:/mnt/optummarkt/202205/ontology/base/Procedure';

select * from ty18_2205_Procedure;

-- COMMAND ----------

drop table if exists ty18_med_proc_subset;

create table ty18_med_proc_subset as
select distinct a.ptid, a.encid, a.proc_date, a.proc_code, a.proc_desc, a.proc_code_type, a.provid_perform, b.*
from ty18_2205_Procedure a join ty13_study_pr_codes b
on a.proc_code=b.code
order by a.ptid, a.proc_date, a.encid
;

select * from ty18_med_proc_subset;

-- COMMAND ----------

select condition, sub_condition, count(*) as n_obs
from ty18_med_proc_subset
group by condition, sub_condition
order by condition, sub_condition
;

-- COMMAND ----------

drop table if exists ty18_proc_ablation;

create table ty18_proc_ablation as
select distinct *
from ty18_med_proc_subset
where sub_condition='Ablation'
order by ptid, proc_date
;

select * from ty18_proc_ablation;

-- COMMAND ----------

drop table if exists ty18_inp_ablation_aad;

create table ty18_inp_ablation_aad as
select a.*, b.year_rx, b.sub_CONDITION as sub_CONDITION_rx, b.dt_1st_rx_aad, b.source, b.encid as encid_inp
from ty18_proc_ablation a join ty18_aad_rx_af_dx_full b
on a.ptid=b.ptid and a.proc_date=b.dt_1st_rx_aad and a.encid=b.encid
order by a.ptid, a.proc_date
;


select count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_rx_aad) as dt_rx_start, max(dt_1st_rx_aad) as dt_rx_end
from ty18_inp_ablation_aad
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
;


-- COMMAND ----------

drop table if exists ac_inp_ablation_aad_test;

create table ac_inp_ablation_aad_test as
select a.*, b.year_rx, b.sub_CONDITION as sub_CONDITION_rx, b.dt_1st_rx_aad, b.source, b.encid as encid_inp, b.visit_start_date,b.visit_end_date
from ty18_proc_ablation a join ty18_aad_rx_af_dx_full b
on a.ptid=b.ptid and a.proc_date=b.dt_1st_rx_aad and a.encid=b.encid
order by a.ptid, a.proc_date
;


-- COMMAND ----------



-- COMMAND ----------

select * from ac_inp_ablation_aad_test
where dt_1st_rx_aad>visit_end_date

-- COMMAND ----------

drop table if exists ty18_ablation_aad_90d;

create table ty18_ablation_aad_90d as
select a.*, b.year_rx, b.sub_condition as sub_condition_rx, b.dt_1st_rx_aad, b.source, b.encid as encid_inp
from ty18_proc_ablation a join ty18_aad_rx_af_dx_full b
on a.ptid=b.ptid and a.proc_date between date_sub(b.dt_1st_rx_aad,90) and date_add(b.dt_1st_rx_aad,90)
order by a.ptid, a.proc_date
;

select * from ty18_ablation_aad_90d;


-- COMMAND ----------

select source, count(*) as n_obs, count(distinct ptid) as n_pat, min(dt_1st_rx_aad) as dt_rx_start, max(dt_1st_rx_aad) as dt_rx_end
from ty18_ablation_aad_90d
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
group by source
;

-- COMMAND ----------

drop table if exists ty18_ablation_any_aad_inp_17_21;

create table ty18_ablation_any_aad_inp_17_21 as
select distinct '*2017-2021' as period, 'any AAD/Ablation' as drug, count(distinct ptid) as n_pat
from ty18_inp_ablation_aad
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
;

select * from ty18_ablation_any_aad_inp_17_21;

-- COMMAND ----------

drop table if exists ty18_ablation_each_aad_inp_17_21;

create table ty18_ablation_each_aad_inp_17_21 as
select distinct '*2017-2021' as period, concat(sub_condition_rx,'/Ablation') as drug, count(distinct ptid) as n_pat
from ty18_inp_ablation_aad
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
group by drug
order by drug
;

select * from ty18_ablation_each_aad_inp_17_21;

-- COMMAND ----------

select distinct '*2017-2021' as period, source,concat(sub_condition_rx,'/Ablation') as drug, count(distinct ptid) as n_pat
from ty18_inp_ablation_aad
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
group by source,drug
order by source,drug
;

-- COMMAND ----------

drop table if exists ty18_ablation_any_aad_90d_17_21;

create table ty18_ablation_any_aad_90d_17_21 as
select distinct '*2017-2021' as period, source, 'any AAD/Ablation' as drug, count(distinct ptid) as n_pat
from ty18_ablation_aad_90d
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
group by source
;

select * from ty18_ablation_any_aad_90d_17_21;

-- COMMAND ----------

drop table if exists ty18_ablation_each_aad_90d_17_21;

create table ty18_ablation_each_aad_90d_17_21 as
select distinct '*2017-2021' as period, source, concat(sub_condition_rx,'/Ablation') as drug, count(distinct ptid) as n_pat
from ty18_ablation_aad_90d
where dt_1st_rx_aad between '2017-01-01' and '2021-12-31'
group by source, drug
order by source, drug
;

select * from ty18_ablation_each_aad_90d_17_21;

-- COMMAND ----------

drop table if exists ty18_n_ablation_aad_inp_all;

create table ty18_n_ablation_aad_inp_all as
select * from ty18_ablation_any_aad_inp_17_21
union
select * from ty18_ablation_each_aad_inp_17_21
order by drug
;

select * from ty18_n_ablation_aad_inp_all;

-- COMMAND ----------

drop table if exists ty18_n_ablation_aad_90d_all;

create table ty18_n_ablation_aad_90d_all as
select * from ty18_ablation_any_aad_90d_17_21
union
select * from ty18_ablation_each_aad_90d_17_21
order by drug
;

select * from ty18_n_ablation_aad_90d_all;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty18_aad_n_all")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").save("/FileStore/tables/ty18_aad_n_all")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty18_n_ablation_aad_inp_all")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").save("/FileStore/tables/ty18_n_ablation_aad_inp_all")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty18_n_ablation_aad_90d_all")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").save("/FileStore/tables/ty18_n_ablation_aad_90d_all")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

----------Import SES Lookup NDC records---------
drop table if exists ty19_ses_2208_lookup_ndc;

create table ty19_ses_2208_lookup_ndc using delta location 'dbfs:/mnt/optumclin/202208/ontology/base/ses/Lookup NDC';

select * from ty19_ses_2208_lookup_ndc;

-- COMMAND ----------

select AHFSCLSS_DESC, count(*) as n_ndc
from ty19_ses_2208_lookup_ndc
group by AHFSCLSS_DESC
order by AHFSCLSS_DESC
;

-- COMMAND ----------

select AHFSCLSS_DESC, GNRC_NM, count(*) as n_ndc
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%DIABETI%' or AHFSCLSS_DESC like '%INSULINS%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'
group by AHFSCLSS_DESC, GNRC_NM
order by AHFSCLSS_DESC, GNRC_NM
;

-- COMMAND ----------

select AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,DRG_STRGTH_NBR,DRG_STRGTH_UNIT_DESC,GNRC_NM,GNRC_SQNC_NBR,NDC,NDC_DRG_ROW_EFF_DT,NDC_DRG_ROW_END_DT,USC_MED_DESC
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%DIABETI%' or AHFSCLSS_DESC like '%INSULINS%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'

order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;

-- COMMAND ----------

drop table if exists ty19_rx_anti_dm_loopup;

create table ty19_rx_anti_dm_loopup as
select distinct AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,DRG_STRGTH_NBR,DRG_STRGTH_UNIT_DESC,GNRC_NM,GNRC_SQNC_NBR,NDC,NDC_DRG_ROW_EFF_DT,NDC_DRG_ROW_END_DT,USC_MED_DESC
      , case when AHFSCLSS_DESC='LONG-ACTING INSULINS' then 'Basal'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') then 'PreMix'
             when AHFSCLSS_DESC like '%INSULIN%' and not(BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') then 'Bolus'
             else null end as rx_type
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%INSULIN%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'
order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;

select *
from ty19_rx_anti_dm_loopup
where AHFSCLSS_DESC like '%INSULINS%'
order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;


-- COMMAND ----------

drop table if exists ty19_rx_anti_dm_loopup;

create table ty19_rx_anti_dm_loopup as
select distinct AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,DRG_STRGTH_NBR,DRG_STRGTH_UNIT_DESC,GNRC_NM,GNRC_SQNC_NBR,NDC,NDC_DRG_ROW_EFF_DT,NDC_DRG_ROW_END_DT,USC_MED_DESC
      , case when AHFSCLSS_DESC='LONG-ACTING INSULINS' or GNRC_NM like '%DETEMIR%' or GNRC_NM like '%GLARGINE%' or GNRC_NM like '%DEGLUDEC%' then 'Basal'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'Inhaler Insulin'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') and not(BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'PreMix'
             when AHFSCLSS_DESC like '%INSULIN%' and not(BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') then 'Bolus'
             else null end as rx_type
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%INSULIN%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'
order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;

select AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,GNRC_NM, rx_type
from ty19_rx_anti_dm_loopup
where AHFSCLSS_DESC like '%INSULINS%'
order by rx_type, AHFSCLSS_DESC, GNRC_NM, BRND_NM
;


-- COMMAND ----------

drop table if exists ty19_rx_anti_dm_loopup;

create table ty19_rx_anti_dm_loopup as
select distinct AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,DRG_STRGTH_NBR,DRG_STRGTH_UNIT_DESC,GNRC_NM,GNRC_SQNC_NBR,NDC,NDC_DRG_ROW_EFF_DT,NDC_DRG_ROW_END_DT,USC_MED_DESC
      , case when AHFSCLSS_DESC='LONG-ACTING INSULINS' or GNRC_NM like '%DETEMIR%' or GNRC_NM like '%GLARGINE%' or GNRC_NM like '%DEGLUDEC%' then 'Basal'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'Inhaler Insulin'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') and not(BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'PreMix'
             when AHFSCLSS_DESC like '%INSULIN%' and not(BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') then 'Bolus'
             else null end as rx_type
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%INSULIN%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'
order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;

select AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,GNRC_NM, rx_type
from ty19_rx_anti_dm_loopup
where
--AHFSCLSS_DESC like '%INSULIN%'
AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
order by rx_type, AHFSCLSS_DESC, GNRC_NM, BRND_NM
;


-- COMMAND ----------

drop table if exists ty19_rx_anti_dm_loopup;

create table ty19_rx_anti_dm_loopup as
select distinct AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,DRG_STRGTH_NBR,DRG_STRGTH_UNIT_DESC,GNRC_NM,GNRC_SQNC_NBR,NDC,NDC_DRG_ROW_EFF_DT,NDC_DRG_ROW_END_DT,USC_MED_DESC
      , case when AHFSCLSS_DESC='LONG-ACTING INSULINS' or GNRC_NM like '%DETEMIR%' or GNRC_NM like '%GLARGINE%' or GNRC_NM like '%DEGLUDEC%' then 'Basal'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'Inhaler Insulin'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') and not(BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'PreMix'
             when AHFSCLSS_DESC like '%INSULIN%' and not(BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') then 'Bolus'
             when AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%' then 'AGI'
             when AHFSCLSS_DESC like '%AMYLIN%' then 'Amylin'
             when AHFSCLSS_DESC like '%BIGUANIDES%' then 'Metformin'
             when AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' and not(GNRC_NM like '%/%') then 'TZD'
             when AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' and GNRC_NM like '%/%' then 'Combinations'
             else null end as rx_type
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%INSULIN%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'
order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;

select AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,GNRC_NM, rx_type
from ty19_rx_anti_dm_loopup
where
--AHFSCLSS_DESC like '%INSULIN%'
--AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
--AHFSCLSS_DESC like '%AMYLIN%'
--AHFSCLSS_DESC like '%BIGUANIDES%'
AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%'
order by rx_type, AHFSCLSS_DESC, GNRC_NM, BRND_NM
;


-- COMMAND ----------

drop table if exists ty19_rx_anti_dm_loopup;

create table ty19_rx_anti_dm_loopup as
select distinct AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,DRG_STRGTH_NBR,DRG_STRGTH_UNIT_DESC,GNRC_NM,GNRC_SQNC_NBR,NDC,NDC_DRG_ROW_EFF_DT,NDC_DRG_ROW_END_DT,USC_MED_DESC
      , case when AHFSCLSS_DESC='LONG-ACTING INSULINS' or GNRC_NM like '%DETEMIR%' or GNRC_NM like '%GLARGINE%' or GNRC_NM like '%DEGLUDEC%' then 'Basal'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'Inhaler Insulin'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') and not(BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'PreMix'
             when AHFSCLSS_DESC like '%INSULIN%' and not(BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') then 'Bolus'
             when AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%' then 'AGI'
             when AHFSCLSS_DESC like '%AMYLIN%' then 'Amylin'
             when AHFSCLSS_DESC like '%BIGUANIDES%' then 'Metformin'
             when AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' and not(GNRC_NM like '%/%') then 'TZD'
             when AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' and GNRC_NM like '%/%' then 'Combinations'
             when AHFSCLSS_DESC like '%DPP-4%' and not(GNRC_NM like '%/%') then 'DPP-4'
             when AHFSCLSS_DESC like '%DPP-4%' and GNRC_NM like '%/%' then 'Combinations'
             else null end as rx_type
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%INSULIN%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'
order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;

select AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,GNRC_NM, rx_type
from ty19_rx_anti_dm_loopup
where
--AHFSCLSS_DESC like '%INSULIN%'
--AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
--AHFSCLSS_DESC like '%AMYLIN%'
--AHFSCLSS_DESC like '%BIGUANIDES%'
--AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%'
 AHFSCLSS_DESC like '%DPP-4%'
order by rx_type, AHFSCLSS_DESC, GNRC_NM, BRND_NM
;


-- COMMAND ----------

drop table if exists ty19_rx_anti_dm_loopup;



-- COMMAND ----------

drop table if exists ty00_ses_rx_anti_dm_loopup;

create table ty00_ses_rx_anti_dm_loopup as
select distinct AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,DRG_STRGTH_NBR,DRG_STRGTH_UNIT_DESC,GNRC_NM,GNRC_SQNC_NBR,NDC,NDC_DRG_ROW_EFF_DT,NDC_DRG_ROW_END_DT,USC_MED_DESC
      , case when AHFSCLSS_DESC='LONG-ACTING INSULINS' or GNRC_NM like '%DETEMIR%' or GNRC_NM like '%GLARGINE%' or GNRC_NM like '%DEGLUDEC%' then 'Basal'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'Inhaler Insulin'
             when AHFSCLSS_DESC like '%INSULIN%' and (BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') and not(BRND_NM like '%EXUBERA%' or BRND_NM like '%AFREZZA%') then 'PreMix'
             when AHFSCLSS_DESC like '%INSULIN%' and not(BRND_NM like '%MIX%' or DRG_STRGTH_DESC like '%-%') then 'Bolus'
             when AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%' then 'AGI'
             when AHFSCLSS_DESC like '%AMYLIN%' then 'Amylin'
             when AHFSCLSS_DESC like '%BIGUANIDES%' then 'Metformin'
             when AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' and not(GNRC_NM like '%/%') then 'TZD'
             when AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' and GNRC_NM like '%/%' then 'Combinations'
             when AHFSCLSS_DESC like '%DPP-4%' and not(GNRC_NM like '%/%') then 'DPP-4'
             when AHFSCLSS_DESC like '%DPP-4%' and GNRC_NM like '%/%' then 'Combinations'
             when AHFSCLSS_DESC like '%SGLT2%' and not(GNRC_NM like '%/%') then 'SGLT2'
             when AHFSCLSS_DESC like '%SGLT2%' and GNRC_NM like '%/%' then 'Combinations'
             when AHFSCLSS_DESC like '%MEGLITINIDE%' and not(GNRC_NM like '%/%') then 'Meglitinide'
             when AHFSCLSS_DESC like '%MEGLITINIDE%' and GNRC_NM like '%/%' then 'Combinations'
             when AHFSCLSS_DESC like '%SULFONYLUREA%' then 'Sulfonylureas'
             when AHFSCLSS_DESC like '%INCRETIN%' then 'GLP1'
             else null end as rx_type
from ty19_ses_2208_lookup_ndc
where AHFSCLSS_DESC like '%INSULIN%' or AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
      or AHFSCLSS_DESC like '%AMYLIN%' or AHFSCLSS_DESC like '%BIGUANIDES%' or AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%' or AHFSCLSS_DESC like '%DPP-4%'
      or AHFSCLSS_DESC like '%SULFONYLUREA%' or AHFSCLSS_DESC like '%SGLT2%' or AHFSCLSS_DESC like '%INCRETIN%' or AHFSCLSS_DESC like '%MEGLITINIDE%'
order by AHFSCLSS_DESC, GNRC_NM, BRND_NM
;

select AHFSCLSS_DESC,BRND_NM,DOSAGE_FM_DESC,DRG_STRGTH_DESC,GNRC_NM, rx_type
from ty00_ses_rx_anti_dm_loopup
--where
--AHFSCLSS_DESC like '%INSULIN%'
--AHFSCLSS_DESC like '%ALPHA-GLUCOSIDASE INHIBITORS%'
--AHFSCLSS_DESC like '%AMYLIN%'
--AHFSCLSS_DESC like '%BIGUANIDES%'
--AHFSCLSS_DESC like '%THIAZOLIDINEDIONE%'
-- AHFSCLSS_DESC like '%DPP-4%'
--AHFSCLSS_DESC like '%SULFONYLUREA%'
--AHFSCLSS_DESC like '%SGLT2%'
--AHFSCLSS_DESC like '%INCRETIN%'
--AHFSCLSS_DESC like '%MEGLITINIDE%'
--isnull(rx_type)
order by rx_type, AHFSCLSS_DESC, GNRC_NM, BRND_NM
;


-- COMMAND ----------

----------Import SES Medical Procedures records---------
drop table if exists ty19_ses_2208_med_proc;

create table ty19_ses_2208_med_proc using delta location 'dbfs:/mnt/optumclin/202208/ontology/base/ses/Medical Procedures';

select * from ty19_ses_2208_med_proc;
select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(fst_dt) as dt_pr_start, max(fst_dt) as dt_pr_stop
from ty19_ses_2208_med_proc;

