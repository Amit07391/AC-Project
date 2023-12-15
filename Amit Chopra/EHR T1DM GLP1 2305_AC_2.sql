-- Databricks notebook source
drop table if exists ac_ehr_rx_anti_dm_2;

create table ac_ehr_rx_anti_dm_2 as
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm,QUANTITY_PER_FILL,
QUANTITY_OF_DOSE,DOSE_FREQUENCY, NUM_REFILLS, DAYS_SUPPLY, 'Pres' as source from ac_rx_pres_anti_dm
union
select ptid, rxdate, ndc, drug_name, generic_desc, drug_class, rx_type, gnrc_nm, brnd_nm,0 as qtry_per_fill,
QUANTITY_OF_DOSE,DOSE_FREQUENCY, 0 as num_refills, 0 as days_supply, 'Admi' as source from ac_rx_admi_anti_dm
order by ptid, rxdate
;

select * from ac_ehr_rx_anti_dm_2;


-- COMMAND ----------

create or replace table ac_ehr_rx_anti_dm_bas_bol as 
select distinct * from ac_ehr_rx_anti_dm_2 
where rx_type in ('Basal','Bolus') and rxdate between '2021-01-01' and '2021-12-31' 
order by ptid, rxdate;

-- , dateadd(rxdate, cast(days_sup as int)) as end_date

select distinct * from ac_ehr_rx_anti_dm_bas_bol
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_rehr_x_anti_dm_bas_bol_1 as
select distinct a.* from ac_ehr_rx_anti_dm_bas_bol a 
join ac_ehr_rx_anti_dm_bas_bol b on a.ptid=b.ptid and a.rx_type<>b.rx_type
where ((a.rxdate between b.rxdate and b.rxdate + 90 ) OR
(b.rxdate between a.rxdate and a.rxdate + 90 ))
order by a.ptid, a.rxdate;

select distinct * from ac_rehr_x_anti_dm_bas_bol_1
order by ptid, rxdate

-- COMMAND ----------

select ptid,count(distinct rxdate) as n_fills, count(distinct rx_type) as n_drugs  from ac_rehr_x_anti_dm_bas_bol_1
group by 1
order by 3 desc;

-- select patid,count(distinct rx_type) as n_drugs  from ac_rehr_x_anti_dm_bas_bol_1
-- group by 1
-- order by 2 asc

-- COMMAND ----------

select distinct * from ac_rehr_x_anti_dm_bas_bol_1
where ptid='PT078334820'
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_ehr_rx_anti_dm_bas_bol_GLP_GIP as 
select distinct a.*, a.rx_type as rx_typ
from ac_ehr_rx_anti_dm_2 a 
inner join ac_rehr_x_anti_dm_bas_bol_1 b on a.ptid=b.ptid
where (a.rx_type in ('Basal','Bolus') or (a.rx_type='GLP1' and lcase(a.brnd_nm) not like '%mounjaro%') or lcase(a.brnd_nm) like '%mounjaro%')
 and a.rxdate between '2021-01-01' and '2021-12-31'
order by a.ptid, a.rxdate;

select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP
order by ptid, rxdate;

-- COMMAND ----------


-- select distinct brnd_nm from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP
--  where
-- lower(brnd_nm) like '%glp%';

select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP
where ptid='PT078334820'
order by ptid, rxdate;
-- select distinct * from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and lower(brnd_nm) like '%mounjaro%' 


-- COMMAND ----------

create or replace table ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_1 as
select distinct *,
case when rx_typ in ('Basal','Bolus') then 'Basal_Basal'
when rx_typ in ('GLP1') and lower(brnd_nm) not like '%mounjaro%' then 'GLP1'
end as Drug_flag from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP
order by ptid, rxdate;

select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_1
-- where patid='33003286489'
order by ptid, rxdate;

-- COMMAND ----------

select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_1
where ptid='PT078254762'
order by ptid, rxdate

-- COMMAND ----------

create or replace table ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_2 as
select distinct a.* from ac_ehr_rx_anti_dm_2 a
join (select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_1 where rx_type='Basal') b on a.ptid=b.ptid
join (select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_1 where rx_type="GLP1") c on b.ptid=c.ptid and b.drug_flag<>c.drug_flag
join (select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_1 where rx_type='Bolus') d on c.ptid=d.ptid and c.drug_flag<>d.drug_flag
where 
((b.rxdate between c.rxdate and c.rxdate + 90) OR
(c.rxdate between b.rxdate and b.rxdate + 90 ) ) 
and
((c.rxdate between d.rxdate and d.rxdate + 90) OR
(d.rxdate between c.rxdate and c.rxdate + 90 ) )
and
((b.rxdate between d.rxdate and d.rxdate + 90) OR
(d.rxdate between b.rxdate and b.rxdate + 90 ) ) 
and a.rx_type in ('Basal','Bolus','GLP1') and a.rxdate between '2021-01-01' and '2021-12-31' 

order by a.ptid, a.rxdate;

select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_2
order by ptid, rxdate ;

-- COMMAND ----------

select distinct * from ac_ehr_rx_anti_dm_bas_bol_GLP_GIP_2
where ptid='PT078343058'
order by ptid, rxdate ;

-- COMMAND ----------

select patid,count(distinct drug_flag) as n_drugs  from ac_rx_anti_dm_bas_bol_GLP_GIP_2
group by 1
order by 2 asc

-- COMMAND ----------

-- MAGIC %md #### Checking the ratio of t1dm and t2dm 

-- COMMAND ----------

create or replace table ac_t1d_t2d_test as
select distinct a.patid, count(distinct a.fst_dt) as t1d_cnts, count(distinct b.fst_dt) as t2d_cnts from
(select distinct patid, fst_dt from ty41_dx_t1dm where fst_dt between '2021-01-01' and '2021-12-31') a
left join (select distinct patid, fst_dt from ac_dx_t2dm where fst_dt between '2021-01-01' and '2021-12-31') b on a.patid=b.patid
group by 1
order by 1;

select distinct * from ac_t1d_t2d_test;

-- COMMAND ----------

create or replace table ac_t1d_t2d_test_2 as
select distinct *, t2d_cnts/t1d_cnts as ratio from ac_t1d_t2d_test
where t2d_cnts/t1d_cnts>=2
order by 1;

select distinct * from ac_t1d_t2d_test_2
order by 1;

-- COMMAND ----------

select count(distinct patid) from ac_t1d_t2d_test_2
