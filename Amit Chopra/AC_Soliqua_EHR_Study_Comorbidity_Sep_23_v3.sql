-- Databricks notebook source
select distinct CATEGORY from sg_antidiabetics_codes

-- COMMAND ----------

drop table if exists ac_rx_pres_2308_anti_dm;

create table ac_rx_pres_2308_anti_dm as
select distinct a.ptid, a.rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.daily_dose, a.dose_frequency, a.quantity_per_fill
, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, b.*
from ac_ehr_WRx_202308 a join sg_antidiabetics_codes b
on a.ndc=b.NDC_Code
order by a.ptid, a.rxdate
;

select * from ac_rx_pres_2308_anti_dm;


-- COMMAND ----------

drop table if exists ac_rx_admi_2308_anti_dm;

create table ac_rx_admi_2308_anti_dm as
select distinct a.ptid, a.admin_date as rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.dose_frequency
, a.generic_desc, a.drug_class, b.*
from ac_ehr_202308_RX_Administration a join sg_antidiabetics_codes b
on a.ndc=b.NDC_Code
order by a.ptid, rxdate
;

select * from ac_rx_admi_2308_anti_dm;


-- COMMAND ----------

drop table if exists ac_rx_anti_dm_2308;

create table ac_rx_anti_dm_2308 as
select ptid, rxdate, NDC_Code, drug_name, generic_desc, drug_class, GENERIC_NAME, CATEGORY, 'Pres' as source from ac_rx_pres_2308_anti_dm
union
select ptid, rxdate, NDC_Code, drug_name, generic_desc, drug_class, GENERIC_NAME, CATEGORY, 'Admi' as source from ac_rx_admi_2308_anti_dm
order by ptid, rxdate
;

select * from ac_rx_anti_dm_2308;

-- select rx_type, drug_name
-- from ac_rx_anti_dm
-- group by rx_type, drug_name
-- order by rx_type, drug_name
-- ;


-- COMMAND ----------

create or replace table ac_ehr_sol_dx_comorb_bl as
select distinct a.*, b.dt_rx_index from ac_ehr_diag_202308 a
join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where a.diag_date between dt_rx_index - 180 and dt_rx_index
order by a.ptid, diag_date;

select distinct * from ac_ehr_sol_dx_comorb_bl
order by ptid, diag_date;

-- COMMAND ----------

create or replace table ac_ehr_sol_dx_comorb_bl_2 as
select distinct * from ac_ehr_sol_dx_comorb_bl
where DIAGNOSIS_STATUS='Diagnosis of' and 
(diagnosis_cd  like  'I10%'  OR
diagnosis_cd  like  'I11%'  OR
diagnosis_cd  like  'I15%'  OR
diagnosis_cd  like  'I674%'  OR
diagnosis_cd  like  'I12%'  OR
diagnosis_cd  like  'I13%'  OR

diagnosis_cd  like  'E780%'  OR
diagnosis_cd  like  'E781%'  OR
diagnosis_cd  like  'E782%'  OR
diagnosis_cd  like  'E783%'  OR
diagnosis_cd  like  'E784%'  OR
diagnosis_cd  like  'E785%'  OR

diagnosis_cd  like  'E0840%'  OR
diagnosis_cd  like  'E0841%'  OR
diagnosis_cd  like  'E0842%'  OR
diagnosis_cd  like  'E0843%'  OR
diagnosis_cd  like  'E0940%'  OR
diagnosis_cd  like  'E0941%'  OR
diagnosis_cd  like  'E0942%'  OR
diagnosis_cd  like  'E0943%'  OR
diagnosis_cd  like  'E1040%'  OR
diagnosis_cd  like  'E1041%'  OR
diagnosis_cd  like  'E1042%'  OR
diagnosis_cd  like  'E1043%'  OR
diagnosis_cd  like  'E1140%'  OR
diagnosis_cd  like  'E1141%'  OR
diagnosis_cd  like  'E1142%'  OR
diagnosis_cd  like  'E1143%'  OR
diagnosis_cd  like  'E1340%'  OR
diagnosis_cd  like  'E1341%'  OR
diagnosis_cd  like  'E1342%'  OR
diagnosis_cd  like  'E1343%'  OR

diagnosis_cd  like  'E0821%'  OR
diagnosis_cd  like  'E0921%'  OR
diagnosis_cd  like  'E1021%'  OR
diagnosis_cd  like  'E1121%'  OR
diagnosis_cd  like  'E1321%'  OR


diagnosis_cd  like  'E0831%'  OR
diagnosis_cd  like  'E0832%'  OR
diagnosis_cd  like  'E0833%'  OR
diagnosis_cd  like  'E0834%'  OR
diagnosis_cd  like  'E0835%'  OR
diagnosis_cd  like  'E0931%'  OR
diagnosis_cd  like  'E0932%'  OR
diagnosis_cd  like  'E0933%'  OR
diagnosis_cd  like  'E0934%'  OR
diagnosis_cd  like  'E0935%'  OR
diagnosis_cd  like  'E1031%'  OR
diagnosis_cd  like  'E1032%'  OR
diagnosis_cd  like  'E1033%'  OR
diagnosis_cd  like  'E1034%'  OR
diagnosis_cd  like  'E1035%'  OR
diagnosis_cd  like  'E1131%'  OR
diagnosis_cd  like  'E1132%'  OR
diagnosis_cd  like  'E1133%'  OR
diagnosis_cd  like  'E1134%'  OR
diagnosis_cd  like  'E1135%'  OR
diagnosis_cd  like  'E1331%'  OR
diagnosis_cd  like  'E1332%'  OR
diagnosis_cd  like  'E1333%'  OR
diagnosis_cd  like  'E1334%'  OR
diagnosis_cd  like  'E1335%'  OR

diagnosis_cd  like  'E660%'  OR
diagnosis_cd  like  'E661%'  OR
diagnosis_cd  like  'E662%'  OR
diagnosis_cd  like  'E668%'  OR
diagnosis_cd  like  'E669%'  OR
diagnosis_cd  like  'Z683%'  OR
diagnosis_cd  like  'Z684%'  OR


diagnosis_cd  like  'I12%'  OR
diagnosis_cd  like  'I13%'  OR
diagnosis_cd  like  'N01%'  OR
diagnosis_cd  like  'N02%'  OR
diagnosis_cd  like  'N03%'  OR
diagnosis_cd  like  'N04%'  OR
diagnosis_cd  like  'N05%'  OR
diagnosis_cd  like  'N06%'  OR
diagnosis_cd  like  'N07%'  OR
diagnosis_cd  like  'N08%'  OR
diagnosis_cd  like  'N11%'  OR
diagnosis_cd  like  'N12%'  OR
diagnosis_cd  like  'N13%'  OR
diagnosis_cd  like  'N14%'  OR
diagnosis_cd  like  'N15%'  OR
diagnosis_cd  like  'N16%'  OR
diagnosis_cd  like  'N18%'  OR
diagnosis_cd  like  'N19%'  OR
diagnosis_cd  like  'Q611%'  OR
diagnosis_cd  like  'Q612%'  OR
diagnosis_cd  like  'Q613%'  OR
diagnosis_cd  like  'Z49%'  OR
diagnosis_cd  like  'Z9115%'  OR
diagnosis_cd  like  'Z94%'  OR
diagnosis_cd  like  'Z992%' OR

diagnosis_cd  like  'F204%'  OR
diagnosis_cd  like  'F313%'  OR
diagnosis_cd  like  'F314%'  OR
diagnosis_cd  like  'F315%'  OR
diagnosis_cd  like  'F32%'  OR
diagnosis_cd  like  'F33%' or
diagnosis_cd  like  'F341%'  OR
diagnosis_cd  like  'F412%'  OR 
diagnosis_cd  like  'F432%'   )
order by ptid, DIAG_DATE;

select distinct * from ac_ehr_sol_dx_comorb_bl_2
order by ptid, DIAG_DATE;

-- COMMAND ----------

create or replace table ac_ehr_sol_dx_comorb_bl_3 as
select distinct *,
case when diagnosis_cd  like  'I10%'  OR
diagnosis_cd  like  'I11%'  OR
diagnosis_cd  like  'I15%'  OR
diagnosis_cd  like  'I674%'  OR
diagnosis_cd  like  'I12%'  OR
diagnosis_cd  like  'I13%'   then 'Hypertension'
when diagnosis_cd  like  'E780%'  OR
diagnosis_cd  like  'E781%'  OR
diagnosis_cd  like  'E782%'  OR
diagnosis_cd  like  'E783%'  OR
diagnosis_cd  like  'E784%'  OR
diagnosis_cd  like  'E785%'  then 'Hyperlipidemia'
when diagnosis_cd  like  'E0840%'  OR
diagnosis_cd  like  'E0841%'  OR
diagnosis_cd  like  'E0842%'  OR
diagnosis_cd  like  'E0843%'  OR
diagnosis_cd  like  'E0940%'  OR
diagnosis_cd  like  'E0941%'  OR
diagnosis_cd  like  'E0942%'  OR
diagnosis_cd  like  'E0943%'  OR
diagnosis_cd  like  'E1040%'  OR
diagnosis_cd  like  'E1041%'  OR
diagnosis_cd  like  'E1042%'  OR
diagnosis_cd  like  'E1043%'  OR
diagnosis_cd  like  'E1140%'  OR
diagnosis_cd  like  'E1141%'  OR
diagnosis_cd  like  'E1142%'  OR
diagnosis_cd  like  'E1143%'  OR
diagnosis_cd  like  'E1340%'  OR
diagnosis_cd  like  'E1341%'  OR
diagnosis_cd  like  'E1342%'  OR
diagnosis_cd  like  'E1343%'  then 'Diabetic Neuropathy'
when diagnosis_cd  like  'E0821%'  OR
diagnosis_cd  like  'E0921%'  OR
diagnosis_cd  like  'E1021%'  OR
diagnosis_cd  like  'E1121%'  OR
diagnosis_cd  like  'E1321%'  then 'Diabetic Nephropathy'
when diagnosis_cd  like  'E0831%'  OR
diagnosis_cd  like  'E0832%'  OR
diagnosis_cd  like  'E0833%'  OR
diagnosis_cd  like  'E0834%'  OR
diagnosis_cd  like  'E0835%'  OR
diagnosis_cd  like  'E0931%'  OR
diagnosis_cd  like  'E0932%'  OR
diagnosis_cd  like  'E0933%'  OR
diagnosis_cd  like  'E0934%'  OR
diagnosis_cd  like  'E0935%'  OR
diagnosis_cd  like  'E1031%'  OR
diagnosis_cd  like  'E1032%'  OR
diagnosis_cd  like  'E1033%'  OR
diagnosis_cd  like  'E1034%'  OR
diagnosis_cd  like  'E1035%'  OR
diagnosis_cd  like  'E1131%'  OR
diagnosis_cd  like  'E1132%'  OR
diagnosis_cd  like  'E1133%'  OR
diagnosis_cd  like  'E1134%'  OR
diagnosis_cd  like  'E1135%'  OR
diagnosis_cd  like  'E1331%'  OR
diagnosis_cd  like  'E1332%'  OR
diagnosis_cd  like  'E1333%'  OR
diagnosis_cd  like  'E1334%'  OR
diagnosis_cd  like  'E1335%' then 'Diabetic Retinopathy'
when diagnosis_cd  like  'I12%'  OR
diagnosis_cd  like  'I13%'  OR
diagnosis_cd  like  'N01%'  OR
diagnosis_cd  like  'N02%'  OR
diagnosis_cd  like  'N03%'  OR
diagnosis_cd  like  'N04%'  OR
diagnosis_cd  like  'N05%'  OR
diagnosis_cd  like  'N06%'  OR
diagnosis_cd  like  'N07%'  OR
diagnosis_cd  like  'N08%'  OR
diagnosis_cd  like  'N11%'  OR
diagnosis_cd  like  'N12%'  OR
diagnosis_cd  like  'N13%'  OR
diagnosis_cd  like  'N14%'  OR
diagnosis_cd  like  'N15%'  OR
diagnosis_cd  like  'N16%'  OR
diagnosis_cd  like  'N18%'  OR
diagnosis_cd  like  'N19%'  OR
diagnosis_cd  like  'Q611%'  OR
diagnosis_cd  like  'Q612%'  OR
diagnosis_cd  like  'Q613%'  OR
diagnosis_cd  like  'Z49%'  OR
diagnosis_cd  like  'Z9115%'  OR
diagnosis_cd  like  'Z94%'  OR
diagnosis_cd  like  'Z992%' then 'CKD'

when diagnosis_cd  like  'F204%'  OR
diagnosis_cd  like  'F313%'  OR
diagnosis_cd  like  'F314%'  OR
diagnosis_cd  like  'F315%'  OR
diagnosis_cd  like  'F32%'  OR
diagnosis_cd  like  'F33%' or
diagnosis_cd  like  'F341%'  OR
diagnosis_cd  like  'F412%'  OR 
diagnosis_cd  like  'F432%' then 'Depression'

when diagnosis_cd  like  'E660%'  OR
diagnosis_cd  like  'E661%'  OR
diagnosis_cd  like  'E662%'  OR
diagnosis_cd  like  'E668%'  OR
diagnosis_cd  like  'E669%'  OR
diagnosis_cd  like  'Z683%'  OR
diagnosis_cd  like  'Z684%'   then 'Obesity'
end as Diag_Flag 
from ac_ehr_sol_dx_comorb_bl_2
order by ptid, DIAG_DATE;

select distinct * from ac_ehr_sol_dx_comorb_bl_3
order by ptid, DIAG_DATE;



-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_dx_comorb_bl_3

-- COMMAND ----------

-- MAGIC %md ### Check for hypoglycemia

-- COMMAND ----------

create or replace table ac_ehr_hypo_dx_comorb_bl as
select distinct a.*, b.dt_rx_index from ac_ehr_diag_202308 a
join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where a.diag_date between dt_rx_index - 180 and dt_rx_index
and a.DIAGNOSIS_CD in ('E0864', 'E08641', 'E08649', 'E0964', 'E09641', 'E09649', 'E1164', 'E11641', 'E11649', 'E1364', 'E13641', 'E13649', 'E15', 'E160', 'E161', 'E162')
order by a.ptid, diag_date;

select distinct * from ac_ehr_hypo_dx_comorb_bl  
order by ptid, diag_date;

-- COMMAND ----------

select distinct diagnosis_cd from ac_ehr_hypo_dx_comorb_bl

-- COMMAND ----------

select distinct test_name from ac_ehr_lab_202308
where lower(test_name) like '%glucose%'

-- COMMAND ----------

create or replace table ac_ehr_hypo_dx_comorb_fu as
select distinct a.*, b.dt_rx_index from ac_ehr_diag_202308 a
join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where a.diag_date between dt_rx_index + 1  and dt_rx_index + 180
and a.DIAGNOSIS_CD in ('E0864', 'E08641', 'E08649', 'E0964', 'E09641', 'E09649', 'E1164', 'E11641', 'E11649', 'E1364', 'E13641', 'E13649', 'E15', 'E160', 'E161', 'E162')
order by a.ptid, diag_date;

select distinct * from ac_ehr_hypo_dx_comorb_fu  
order by ptid, diag_date;

-- COMMAND ----------

create or replace table ac_ehr_soliqua_hypo_lab_1 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date, cast(test_result as double) as result, b.dt_rx_index from ac_ehr_lab_202308 a
join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where 
 a.TEST_NAME in ('Glucose.fasting', 'Glucose.postprandial', 'Glucose.random');

select distinct * from ac_ehr_soliqua_hypo_lab_1  
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_soliqua_hypo_lab_2 as
select distinct * from ac_ehr_soliqua_hypo_lab_1
where result_unit= 'MG/DL' and result < 70
order by ptid, service_date;

select distinct * from ac_ehr_soliqua_hypo_lab_2
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_ehr_soliqua_hypo_lab_dx_bl as
select distinct ptid, encid, DIAG_DATE, 'dx' as flag   from ac_ehr_hypo_dx_comorb_bl
union
select distinct ptid,encid, service_date, 'lab' as flag from ac_ehr_soliqua_hypo_lab_2
where service_date between dt_rx_index - 180 and dt_rx_index;

select distinct * from ac_ehr_soliqua_hypo_lab_dx_bl
order by ptid, diag_date;

-- COMMAND ----------

-- MAGIC %md #### hospitalizations due to hypo

-- COMMAND ----------

drop table if exists ac_dx_lab_any_hypo_inp;

create table ac_dx_lab_any_hypo_inp as
select distinct a.*, b.interaction_type, b.visitid, b.interaction_date, c.visit_type, c.visit_start_date, c.visit_end_date, c.discharge_disposition, c.admission_source, c.drg, c.fl_inp
from ac_ehr_soliqua_hypo_lab_dx_bl a left join (select * from ac_ehr_enc_202308 where interaction_type='Inpatient') b on a.ptid=b.ptid and a.encid=b.encid
                            left join (select *, 1 as fl_inp from ac_ehr_Visit_202308 where visit_type='Inpatient' and isnotnull(visit_start_date)) c on a.ptid=c.ptid and b.visitid=c.visitid
order by a.ptid
;

select * from ac_dx_lab_any_hypo_inp;


-- COMMAND ----------

select distinct ptid from ac_dx_lab_any_hypo_inp
where fl_inp=1 and diag_date between visit_start_date and visit_end_date

-- COMMAND ----------

create or replace table ac_ehr_soliqua_hypo_lab_dx_fu as
select distinct ptid, DIAG_DATE, 'dx' as flag   from ac_ehr_hypo_dx_comorb_fu
union
select distinct ptid, service_date, 'lab' as flag from ac_ehr_soliqua_hypo_lab_2
where service_date between dt_rx_index + 1 and dt_rx_index + 180;

select distinct * from ac_ehr_soliqua_hypo_lab_dx_fu
order by ptid, diag_date;

-- COMMAND ----------

-- MAGIC %md #### Using CCI

-- COMMAND ----------

create table ac_dx_subset_CCI_soliq_6m_bl_pts as
select distinct a.*, b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_ehr_sol_dx_comorb_bl a join ty00_all_dx_comorb b
on a.DIAGNOSIS_CD=b.code
where upper(b.dx_name) like '%CCI%'
order by a.ptid, a.DIAG_DATE
;
select * from ac_dx_subset_CCI_soliq_6m_bl_pts
order by ptid, diag_date;

-- COMMAND ----------

create or replace table ac_dx_sol_bl_cci_pat as
select distinct ptid, dx_name, Disease, weight_old from ac_dx_subset_CCI_soliq_6m_bl_pts
where DIAG_DATE between date_sub(dt_rx_index,180) and dt_rx_index
order by 1,2,3,4;

select distinct * from ac_dx_sol_bl_cci_pat
order by 1,2,3,4;

-- COMMAND ----------

create or replace table ac_dx_sol_bl_cci_pat_2 as
select distinct ptid, dx_name, disease, sum(weight_old) as weight_new from ac_dx_sol_bl_cci_pat
group by 1,2,3
order by 1,2,3;

select distinct * from ac_dx_sol_bl_cci_pat_2

-- COMMAND ----------

select distinct dx_name, disease from ac_dx_sol_bl_cci_pat
order by 1

-- COMMAND ----------

select dx_name, ptid, count( *) from ac_dx_sol_bl_cci_pat
group by 1,2
order by 1,2

-- COMMAND ----------

create or replace table ac_bl_sol_dx_cci_mean as
select distinct 'Baseline [-180,0]' as period, 'CCI Score' as cat3, 'Mean of CCI Score' as description, count(distinct ptid) as n, mean(cci_score) as mean, std(cci_score) as std, min(cci_score) as min
        , percentile(cci_score,.25) as p25, percentile(cci_score,.5) as median, percentile(cci_score,.75) as p75, max(cci_score) as max
from (select ptid, sum(weight_new) as cci_score from ac_dx_sol_bl_cci_pat_2 group by ptid)
group by cat3, description
order by cat3, description
;

select * from ac_bl_sol_dx_cci_mean;

-- COMMAND ----------

create or replace table ac_bl_dx_sol_cci_class as
select distinct 'Baseline [-180,30]' as period, 'CCI Score Class' as cat3
, case when cci_score<=0 then 'CCI Score: <=0'
       when cci_score=1 then 'CCI Score: 1'
       when cci_score=2 then 'CCI Score: 2'
       when cci_score>=3 then 'CCI Score:>= 3'
       end as description, count(distinct ptid) as n
from (select ptid, sum(weight_new) as cci_score from ac_dx_sol_bl_cci_pat_2 group by ptid)
group by cat3, description
order by cat3, description
;

select distinct * from ac_bl_dx_sol_cci_class
order by 1,2;
