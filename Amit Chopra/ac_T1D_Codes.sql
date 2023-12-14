-- Databricks notebook source
create table ac00_all_dx_comorb as
select distinct * from ty00_all_dx_comorb;

-- COMMAND ----------

select distinct dx_name from ac00_all_dx_comorb
order by dx_name

-- COMMAND ----------

select distinct * from ac00_all_dx_comorb
where dx_name='T2DM'

-- COMMAND ----------

create table ac00_ses_rx_anti_dm_loopup as
select distinct * from ty00_ses_rx_anti_dm_loopup

-- COMMAND ----------

select distinct * from ac00_ses_rx_anti_dm_loopup


-- COMMAND ----------

select distinct * from ac00_ses_rx_anti_dm_loopup
where rx_type='Sulfonylureas'
