-- Databricks notebook source
select distinct * from sg_antidiabetics_codes

-- COMMAND ----------

select distinct gnrc_nm from ty00_ses_rx_anti_dm_loopup
where rx_type="Bolus"

-- COMMAND ----------

create or replace table ac_rx_anti_dm_lookup as
select distinct a.*, b.category from ty00_ses_rx_anti_dm_loopup a
left join sg_antidiabetics_codes b on a.ndc=b.ndc_code;

select distinct * from ac_rx_anti_dm_lookup;

-- COMMAND ----------

drop table if exists ac_rx_pres_2308_anti_dm;

create table ac_rx_pres_2308_anti_dm as
select distinct a.ptid, a.rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.daily_dose, a.dose_frequency, a.quantity_per_fill
, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, b.*
from ac_ehr_WRx_202308 a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.NDC
order by a.ptid, a.rxdate
;

select * from ac_rx_pres_2308_anti_dm;


-- COMMAND ----------

drop table if exists ac_rx_admi_2308_anti_dm;

create table ac_rx_admi_2308_anti_dm as
select distinct a.ptid, a.admin_date as rxdate, a.drug_name, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form, a.dose_frequency
, a.generic_desc, a.drug_class, b.*
from ac_ehr_202308_RX_Administration a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.NDC
order by a.ptid, rxdate
;

select * from ac_rx_admi_2308_anti_dm;


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

create or replace table ac_rx_pres_2308_anti_dm_bl as 
select distinct a.*, b.dt_rx_index
from ac_rx_pres_2308_anti_dm a
join  ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where a.rxdate between dt_rx_index - 180 and dt_rx_index
order by a.ptid, rxdate;

select distinct * from ac_rx_pres_2308_anti_dm_bl
where rx_type in ('Basal','Bolus')
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_rx_admi_2308_anti_dm_bl as 
select distinct a.*, b.dt_rx_index
from ac_rx_admi_2308_anti_dm a
join  ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where a.rxdate between dt_rx_index - 180 and dt_rx_index
order by a.ptid, rxdate;

select distinct * from ac_rx_admi_2308_anti_dm_bl
where rx_type in ('Basal','Bolus')
order by ptid, rxdate;



-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_bl as
select distinct a.*, b.dt_rx_index from ac_rx_anti_dm_16_23 a
join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where a.rxdate between dt_rx_index - 180 and dt_rx_index
order by a.ptid, rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_bl
order by ptid, rxdate;

-- COMMAND ----------

select distinct rx_type from ac_ehr_sol_rx_anti_dm_bl

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_bl_2 as
select distinct *, 
case when lower(rx_type) like '%metformin%' or lower(rx_type) like '%sglt%' or rx_type= 'AGI' or rx_type='DPP-4'
or rx_type ='TZD' or rx_type = 'Meglitinide' or rx_type = 'Sulfonylureas' then 'OAD'
else rx_type end as Drug_cat
from ac_ehr_sol_rx_anti_dm_bl
order by ptid, rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_bl_2
order by ptid, rxdate;

-- COMMAND ----------

select distinct * from ac_ehr_sol_rx_anti_dm_bl_2
where ptid='PT087951420';

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_2
where ptid not in (select distinct ptid from ac_ehr_sol_rx_anti_dm_bl_2 where Drug_cat='OAD');

-- COMMAND ----------

-- create or replace table ac_ehr_sol_OAD_use_bl as
-- select distinct ptid, count(distinct rx_type) as cnt from ac_ehr_sol_rx_anti_dm_bl_2
-- where drug_cat='OAD'
-- group by ptid
-- order by ptid;

-- select distinct * from ac_ehr_sol_OAD_use_bl
-- order by ptid;

-- select count(distinct ptid) from ac_ehr_sol_OAD_use_bl
-- where cnt>=3;

-- select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_2
-- where drug_cat='OAD';

select rx_type, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_2
group by 1
order by 1 ;

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_bl_demo as
select distinct a.*, b.gender, b.age_index, b.value1  from ac_ehr_sol_rx_anti_dm_bl_2 a
left join ac_ehr_soliqua_demo_hba1c_bl_pts_2 b on a.ptid=b.ptid
order by a.ptid, a.rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_bl_demo
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_demo
where drug_cat='OAD' and age_index>=65;

select rx_type, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_demo
where age_index>=65
group by 1
order by 1 ;

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_demo
where drug_cat='OAD' and value1>=7;

select rx_type, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_demo
where value1>=7
group by 1
order by 1 ;

create or replace table ac_ehr_sol_OAD_use_demo_bl as
select distinct ptid, count(distinct rx_type) as cnt from ac_ehr_sol_rx_anti_dm_bl_demo
where drug_cat='OAD' and age_index>=65
group by ptid
order by ptid;

select distinct * from ac_ehr_sol_OAD_use_demo_bl
order by ptid;

select count(distinct ptid) from ac_ehr_sol_OAD_use_demo_bl
where cnt>=3;



-- COMMAND ----------

-- select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_demo
-- where ptid not in (select distinct ptid from ac_ehr_sol_rx_anti_dm_bl_demo where Drug_cat='OAD' and age_index>=65)
-- and age_index>=65;

-- select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_demo
-- where ptid not in (select distinct ptid from ac_ehr_sol_rx_anti_dm_bl_demo where Drug_cat='OAD' and value1>=7)
-- and value1>=7;

-- create or replace table ac_ehr_sol_OAD_use_hba1c_bl as
-- select distinct ptid, count(distinct rx_type) as cnt from ac_ehr_sol_rx_anti_dm_bl_demo
-- where drug_cat='OAD' and value1>=7
-- group by ptid
-- order by ptid;

-- select distinct * from ac_ehr_sol_OAD_use_hba1c_bl
-- order by ptid;

select count(distinct ptid) from ac_ehr_sol_OAD_use_hba1c_bl
where cnt>=3;


-- COMMAND ----------

-- MAGIC %md #### Basal Analysis

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_bl_basal as
select distinct *,
case when lcase(brnd_nm) like '%toujeo%' then 'Toujeo'
when lcase(gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(brnd_nm) not like '%toujeo%' then 'Gla-100'
when lcase(gnrc_nm) like '%detemir%' then 'Detemir'
when lcase(gnrc_nm) like '%insulin degludec%' and lcase(brnd_nm) like '%tresiba%' then 'DEGLUDEC'
else rx_type end as rx_type2
from ac_ehr_sol_rx_anti_dm_bl_demo
where rx_type='Basal'
order by ptid, rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_bl_basal
order by ptid, rxdate;

-- COMMAND ----------

select distinct gnrc_nm from ac_ehr_sol_rx_anti_dm_bl_basal

-- COMMAND ----------

select distinct brnd_nm, gnrc_nm from ac_ehr_sol_rx_anti_dm_bl_basal
where gnrc_nm='INSULIN DEGLUDEC'

-- COMMAND ----------

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_basal
group by 1
order by 1;

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_basal
where age_index>=65
group by 1
order by 1;


select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_basal
where value1>=7
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_bl_bolus as
select distinct *,
case when gnrc_nm like '%INSULIN REGULAR%' then 'INSULIN REGULAR'
else gnrc_nm end as rx_type2
from ac_ehr_sol_rx_anti_dm_bl_demo
where rx_type='Bolus'
order by ptid, rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_bl_bolus
order by ptid, rxdate;

-- COMMAND ----------

select distinct gnrc_nm from ac_ehr_sol_rx_anti_dm_bl_bolus

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_bolus;

select distinct rx_type2 from ac_ehr_sol_rx_anti_dm_bl_bolus;

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_bolus
group by 1
order by 1;


select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_bolus
where age_index>=65
group by 1
order by 1;

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_bolus
where value1>=7
group by 1
order by 1;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_bolus
where value1>=7
