-- Databricks notebook source
create or replace table ac_ehr_soliqua_demo_no_glp1_prmx_bl as
select distinct a.* from ac_ehr_soliqua_demo_hba1c_bl_pts a
inner join ac_ehr_sol_rx_no_glp_prmx_bl c on a.ptid=c.ptid
order by ptid;
select distinct * from ac_ehr_soliqua_demo_no_glp1_prmx_bl
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_soliqua_demo_no_glp1_prmx_bl_2 as
select distinct *, case when BIRTH_YR='1934 and Earlier' then 1934 else cast(birth_yr as int) end as Birth_YEAR1,
case when race='Caucasian' and ethnicity in ('Not Hispanic','Unknown') then 'Caucasian'
when race='African American' and ethnicity in ('Not Hispanic','Unknown') then 'African American'
when race='Asian' and ethnicity in ('Not Hispanic','Unknown') then 'Asian'
when race in ('Asian','Caucasian','African American','Other/Unknown') and ethnicity ='Hispanic' then 'Hispanic'
else 'Other/Unknown' end as race_new
from ac_ehr_soliqua_demo_no_glp1_prmx_bl
order by 1;

select distinct * from ac_ehr_soliqua_demo_no_glp1_prmx_bl_2
order by 1;

-- COMMAND ----------

select mean(age_index) as mn, median(age_index) as med, std(age_index) as std, min(age_index) as min
        , percentile(age_index,.25) as p25, percentile(age_index,.5) as median, percentile(age_index,.75) as p75, max(age_index) as max  from ac_ehr_soliqua_demo_no_glp1_prmx_bl;

-- COMMAND ----------

-- select case when age_index >=18 and age_index<=49 then '18-49'
-- when age_index >=50 and age_index<=64 then '50-64'
-- when age_index >=65 and age_index<=74 then '65-74'
-- when age_index >=75 then '>=75' end as age_flag, count(distinct ptid) as pts
-- from ac_ehr_soliqua_demo_hba1c_bl_pts
-- group by 1
-- order by 1;

select case when age_index >=18 and age_index<=49 then '18-49'
when age_index >=50 and age_index<=64 then '50-64'
when age_index >=65 and age_index<=74 then '65-74'
when age_index >=75 then '>=75' end as age_flag, count(distinct ptid) as pts
from ac_ehr_soliqua_demo_no_glp1_prmx_bl
group by 1
order by 1;



select gender, count(distinct ptid) as pts
from ac_ehr_soliqua_demo_no_glp1_prmx_bl
group by 1
order by 1;

select race_new, count(distinct ptid) as pts
from ac_ehr_soliqua_demo_no_glp1_prmx_bl_2
group by 1
order by 1;

select region, count(distinct ptid) as pts
from ac_ehr_soliqua_demo_no_glp1_prmx_bl_2
group by 1
order by 1;

select year(dt_rx_index)as yr, count(distinct ptid) as pts
from ac_ehr_soliqua_demo_no_glp1_prmx_bl
group by 1
order by 1;



-- COMMAND ----------

-- MAGIC %md #### Checking weight

-- COMMAND ----------

create or replace table ac_ehr_soli_demo_no_glp_prmx_wt_bmi_bl as
select distinct a.* from ac_ehr_soli_demo_hba1c_wt_bmi_bl a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
order by a.ptid;
select distinct * from ac_ehr_soli_demo_no_glp_prmx_wt_bmi_bl
order by 1;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_soli_demo_no_glp_prmx_wt_bmi_bl

-- COMMAND ----------

select mean(bl_BMI) as mn, median(bl_BMI) as med, std(bl_BMI) as std, min(bl_BMI) as min
        , percentile(bl_BMI,.25) as p25, percentile(bl_BMI,.5) as median, percentile(bl_BMI,.75) as p75, max(bl_BMI) as max  from ac_ehr_soli_demo_no_glp_prmx_wt_bmi_bl;

-- COMMAND ----------

select case when bl_bmi<25 then '<25'
when bl_bmi>=25 and bl_bmi<=29 then '25-29'
when bl_bmi>=30 and bl_bmi<=34 then '30-34'
when bl_bmi>=35 then '>=35'
else 'null' end as BMI_Flag, count(distinct ptid) as pts from ac_ehr_soli_demo_no_glp_prmx_wt_bmi_bl
group by 1
order by 1;



-- COMMAND ----------

select mean(bl_WT) as mn, median(bl_WT) as med, std(bl_WT) as std, min(bl_WT) as min
        , percentile(bl_WT,.25) as p25, percentile(bl_WT,.5) as median, percentile(bl_WT,.75) as p75, max(bl_WT) as max  from ac_ehr_soli_demo_no_glp_prmx_wt_bmi_bl;


-- COMMAND ----------

select case when bl_WT<70 then '<70'
when bl_WT>=70 and bl_WT<100 then '70-99'
when bl_WT>=100 and bl_WT<125 then '100-124'
when bl_WT>=125 then '>=125'
else 'null' end as WT_Flag, count(distinct ptid) as pts from ac_ehr_soli_demo_no_glp_prmx_wt_bmi_bl
group by 1
order by 1;

-- COMMAND ----------

-- MAGIC %md #### Comorbidities

-- COMMAND ----------

create or replace table ac_ehr_soli_demo_comorb_bl as
select distinct a.*, b.Diag_Flag  from ac_ehr_soli_demo_hba1c_wt_bmi_bl a
left join  ac_ehr_sol_dx_comorb_bl_3 b on a.ptid=b.PTID
order by a.ptid;

select distinct * from ac_ehr_soli_demo_comorb_bl
order by ptid;

-- COMMAND ----------

create or replace table ac_ehr_soli_demo_comorb_no_glp_prmx_bl as
select distinct a.* from ac_ehr_soli_demo_comorb_bl a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
order by a.ptid;

select distinct * from ac_ehr_soli_demo_comorb_no_glp_prmx_bl
order by ptid;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_soli_demo_comorb_no_glp_prmx_bl

-- COMMAND ----------

select distinct diag_flag, count(distinct ptid) as pts from ac_ehr_soli_demo_comorb_no_glp_prmx_bl
group by 1
order by 1;



-- COMMAND ----------

-- MAGIC %md #### CCI

-- COMMAND ----------



create or replace table ac_ehr_soli_demo_CCI_no_glp1_prmx_bl as
select distinct a.ptid, b.dx_name,b.Disease, b.weight_old  from ac_ehr_sol_rx_no_glp_prmx_bl a
left join  ac_dx_sol_bl_cci_pat b on a.ptid=b.PTID
order by a.ptid;

select distinct * from ac_ehr_soli_demo_CCI_no_glp1_prmx_bl   
order by ptid;

-- COMMAND ----------



create or replace table ac_ehr_soli_demo_CCI_no_glp1_prmx_bl_2 as
select distinct ptid, dx_name, disease, sum(weight_old) as weight_new from ac_ehr_soli_demo_CCI_no_glp1_prmx_bl
group by 1,2,3
order by 1,2,3;

select distinct * from ac_ehr_soli_demo_CCI_no_glp1_prmx_bl_2

-- COMMAND ----------


select distinct 'Baseline [-180,0]' as period, 'CCI Score' as cat3, 'Mean of CCI Score' as description, count(distinct ptid) as n, mean(cci_score) as mean, std(cci_score) as std, min(cci_score) as min
        , percentile(cci_score,.25) as p25, percentile(cci_score,.5) as median, percentile(cci_score,.75) as p75, max(cci_score) as max
from (select ptid, sum(weight_new) as cci_score from ac_ehr_soli_demo_CCI_no_glp1_prmx_bl_2 group by ptid)
group by cat3, description
order by cat3, description
;


select distinct 'Baseline [-180,30]' as period, 'ECI Score Class' as cat3
, case when cci_score<=0 then 'CCI Score: <=0'
       when cci_score=1 then 'CCI Score: 1'
       when cci_score=2 then 'CCI Score: 2'
       when cci_score>=3 then 'ECI Score:>= 3'
       end as description, count(distinct ptid) as n
from (select ptid, sum(weight_new) as cci_score from ac_ehr_soli_demo_CCI_no_glp1_prmx_bl_2
 group by ptid)
group by cat3, description
order by cat3, description
;



-- COMMAND ----------

select case when value1 < 7 then '<7'
when value1>=7 and value1<8 then '>=7'
when value1>=8 and value1<9 then '>=8'
when value1>=9 then '>=9' end as Hba1c_val, count(distinct ptid) as pts
from ac_ehr_soliqua_demo_no_glp1_prmx_bl
group by 1
order by 1;

select count(distinct ptid) as pts from ac_ehr_soliqua_demo_no_glp1_prmx_bl
where value1<7.5;

select count(distinct ptid) as pts from ac_ehr_soliqua_demo_no_glp1_prmx_bl
where value1<8;

  select mean(value1) as mn, median(value1) as med, std(value1) as std, min(value1) as min
        , percentile(value1,.25) as p25, percentile(value1,.5) as median, percentile(value1,.75) as p75, max(value1) as max  from ac_ehr_soliqua_demo_no_glp1_prmx_bl;

-- COMMAND ----------

-- MAGIC %md #### Hypoglycemia events

-- COMMAND ----------



select count(distinct a.ptid) from ac_ehr_soliqua_hypo_lab_dx_bl a
inner join ac_ehr_soliqua_demo_no_glp1_prmx_bl b on a.ptid=b.ptid;


select count(distinct a.DIAG_DATE) from ac_ehr_soliqua_hypo_lab_dx_bl a
inner join ac_ehr_soliqua_demo_no_glp1_prmx_bl b on a.ptid=b.ptid;



-- COMMAND ----------

select count(distinct ptid) from ac_ehr_soliqua_hypo_lab_dx_fu;

select count(distinct DIAG_DATE) from ac_ehr_soliqua_hypo_lab_dx_fu;
