-- Databricks notebook source
-- MAGIC %md #### Checking data for insulin pump

-- COMMAND ----------

create or replace table ac_ehr_insulin_WRX_1 as
select distinct a.ptid, a.rxdate, a.drug_name, a.ndc, a.ndc_source, a.provid, a.route, a.quantity_of_dose, a.strength, a.strength_unit, a.dosage_form
, a.daily_dose, a.dose_frequency, a.quantity_per_fill, a.num_refills, a.days_supply, a.generic_desc, a.drug_class, a.discontinue_reason, year(a.rxdate) as year, b.AHFSCLSS_DESC, b.rx_type
from ac_Rx_Presc_ehr_202211 a join ty00_ses_rx_anti_dm_loopup b 
on a.ndc=b.ndc 
order by a.ptid, a.rxdate
;

select distinct * from ac_ehr_insulin_WRX_1
order by ptid, rxdate;

-- COMMAND ----------



-- COMMAND ----------

create or replace table ac_ehr_insulin_med_admn_1 as
select distinct a.ptid, a.encid, a.drug_name, a.ndc, a.ndc_source, a.order_date, a.admin_date, a.provid, a.route, a.quantity_of_dose, a.strength
, a.strength_unit, a.dosage_form, a.dose_frequency, a.generic_desc, a.drug_class, a.discontinue_reason, year(a.admin_date) as year, b.AHFSCLSS_DESC, b.rx_type
from ac_Med_Admn_ehr_202211 a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.ptid, a.admin_date
;


select distinct * from ac_ehr_insulin_med_admn_1
order by ptid, admin_date;

-- COMMAND ----------

drop table ac_ehr_insulin_WRX_T1D;

-- create or replace table ac_ehr_insulin_WRX_T1D as
-- select distinct a.*, b.dt_1st_dx_t1d
-- from ac_ehr_insulin_WRX_1 a inner join
-- ac_ehr_t1d_202211_diag_indx b on a.ptid=b.ptid
-- where a.rxdate>=b.dt_1st_dx_t1d
-- order by ptid, rxdate;

-- select distinct * from ac_ehr_insulin_WRX_T1D
-- order by ptid, rxdate;

-- COMMAND ----------

-- MAGIC %md #### Combining the WRx and Med Admin tables

-- COMMAND ----------

create or replace table ac_ehr_insulin_RAI_med_admn_WRx as
select distinct ptid, rxdate, 'Rx' as Flag from ac_ehr_insulin_WRX_1
where NDC in ('00002751001',	'00002751017',	'00002751099',	'00002751559',	'00002751601',	'00002751659',	'00002751699',	'00002872501',	'00002872559',	'00002872599',	'00074751001',	'00088250000',	'00088250001',	'00088250033',	'00088250034',	'00088250052',	'00088250201',	'00088250205',	'00110530401',	'00169330311',	'00169330312',	'00169330390',	'00169330391',	'00169633800',	'00169633810',	'00169633897',	'00169633898',	'00169633910',	'00169633997',	'00169633998',	'00169750111',	'00169750112',	'00169750190',	'00409256710',	'00420330312',	'00420330390',	'00420330391',	'00420633910',	'00420633990',	'00420750111',	'00420750190',	'12854033700',	'12854033701',	'12854033733',	'12854033734',	'12854033752',	'12854033805',	'35356010200',	'50090137500',	'50090166400',	'50090166500',	'50090167800',	'54569643500',	'54569658400',	'54569658500',	'54569658600',	'54569658700',	'54868277700',	'54868510800',	'54868583600',	'54868589900',	'54868605400',	'55045360201',	'62381751509',	'62381751601',	'62381751605',	'62381751609',	'62381872505',	'62381872509',	'64725075001',	'66143751005',	'68115074610',	'68258889903',	'68258892803',	'68258896701')
union
select distinct ptid, admin_date, 'Admin' as Flag from ac_ehr_insulin_med_admn_1
where NDC in ('00002751001',	'00002751017',	'00002751099',	'00002751559',	'00002751601',	'00002751659',	'00002751699',	'00002872501',	'00002872559',	'00002872599',	'00074751001',	'00088250000',	'00088250001',	'00088250033',	'00088250034',	'00088250052',	'00088250201',	'00088250205',	'00110530401',	'00169330311',	'00169330312',	'00169330390',	'00169330391',	'00169633800',	'00169633810',	'00169633897',	'00169633898',	'00169633910',	'00169633997',	'00169633998',	'00169750111',	'00169750112',	'00169750190',	'00409256710',	'00420330312',	'00420330390',	'00420330391',	'00420633910',	'00420633990',	'00420750111',	'00420750190',	'12854033700',	'12854033701',	'12854033733',	'12854033734',	'12854033752',	'12854033805',	'35356010200',	'50090137500',	'50090166400',	'50090166500',	'50090167800',	'54569643500',	'54569658400',	'54569658500',	'54569658600',	'54569658700',	'54868277700',	'54868510800',	'54868583600',	'54868589900',	'54868605400',	'55045360201',	'62381751509',	'62381751601',	'62381751605',	'62381751609',	'62381872505',	'62381872509',	'64725075001',	'66143751005',	'68115074610',	'68258889903',	'68258892803',	'68258896701');

select distinct * from ac_ehr_insulin_RAI_med_admn_WRx
order by ptid, rxdate;

-- COMMAND ----------

-- MAGIC %md #### Creating first month and last month active dates

-- COMMAND ----------

create or replace table ac_Patient_ehr_202211_t1d as
select distinct a.*,left(b.FIRST_MONTH_ACTIVE,4) as first_mnth_yr, right(b.FIRST_MONTH_ACTIVE,2) as first_mnth,
left(b.LAST_MONTH_ACTIVE,4) as last_mnth_yr, right(b.LAST_MONTH_ACTIVE,2) as last_mnth from ac_ehr_dx_t1d_hypo a left join ac_Patient_ehr_202211 b on a.ptid=b.ptid
where a.dx_name='T1DM';

select distinct * from ac_Patient_ehr_202211_t1d;

create or replace table ac_Patient_ehr_202211_t1d_1 as
select distinct *, cast(concat(first_mnth_yr,'-',first_mnth,'-','01') as date) as first_month_active_new,
cast(concat(last_mnth_yr,'-',last_mnth,'-','01') as date) as last_month_active_new from ac_Patient_ehr_202211_t1d;


-- COMMAND ----------

select distinct * from ac_Patient_ehr_202211_t1d_1;
