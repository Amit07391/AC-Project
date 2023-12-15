-- Databricks notebook source
create or replace table ac_rx_anti_dm_bas_bol as 
select distinct *, dateadd(fill_dt, cast(days_sup as int)) as end_date from ty41_rx_anti_dm 
where rx_type in ('Basal','Bolus') and fill_dt between '2021-01-01' and '2021-12-31' 
order by patid, fill_dt;

-- COMMAND ----------



-- COMMAND ----------

create or replace table ac_rx_anti_dm_bas_bol_1 as
select distinct a.* from ac_rx_anti_dm_bas_bol a 
join ac_rx_anti_dm_bas_bol b on a.patid=b.patid and a.rx_type<>b.rx_type
where ((a.FILL_DT between b.FILL_DT and b.end_date + 90 ) OR
(b.FILL_DT between a.FILL_DT and a.end_date + 90 ))
order by a.patid, a.fill_dt;

-- COMMAND ----------

select patid,count(distinct fill_dt) as n_fills, count(distinct rx_type) as n_drugs  from ac_rx_anti_dm_bas_bol_1
group by 1
order by 2 asc;

select patid,count(distinct rx_type) as n_drugs  from ac_rx_anti_dm_bas_bol_1
group by 1
order by 2 asc

-- COMMAND ----------

select distinct * from ac_rx_anti_dm_bas_bol_1
where patid='33198760991'
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_rx_anti_dm_bas_bol_GLP_GIP as 
select distinct a.*, a.rx_type as rx_typ, dateadd(a.fill_dt, cast(a.days_sup as int)) as end_date
from ty41_rx_anti_dm a 
inner join ac_rx_anti_dm_bas_bol_1 b on a.PATID=b.PATID
where (a.rx_type in ('Basal','Bolus') or (a.rx_type='GLP1' and lcase(a.brnd_nm) not like '%mounjaro%') or lcase(a.brnd_nm) like '%mounjaro%')
 and a.fill_dt between '2021-01-01' and '2021-12-31'
order by a.patid, a.fill_dt;

select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP
order by patid, fill_dt;

-- COMMAND ----------


-- select distinct brnd_nm from ac_rx_anti_dm_bas_bol_GLP_GIP where
-- lower(brnd_nm) like '%mounjaro%';

select distinct * from ty41_rx_anti_dm where fill_dt between '2021-01-01' and '2021-12-31' and lower(brnd_nm) like '%mounjaro%' 


-- COMMAND ----------

create or replace table ac_rx_anti_dm_bas_bol_GLP_GIP_1 as
select distinct *,
case when rx_typ in ('Basal','Bolus') then 'Basal_Basal'
when rx_typ in ('GLP1') and lower(brnd_nm) not like '%mounjaro%' then 'GLP1'
end as Drug_flag from ac_rx_anti_dm_bas_bol_GLP_GIP
order by patid, fill_dt;

select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP_1
where patid='33003286489'
order by patid, fill_dt ;

-- COMMAND ----------

-- create or replace table ac_rx_anti_dm_bas_bol_GLP_GIP_2 as
-- select distinct a.* from ac_rx_anti_dm_bas_bol_GLP_GIP_1 a 
-- join ac_rx_anti_dm_bas_bol_GLP_GIP_1 b on a.patid=b.patid and a.drug_flag<>b.drug_flag
-- where ((a.FILL_DT between b.FILL_DT and b.end_date + 90 ) OR
-- (b.FILL_DT between a.FILL_DT and a.end_date + 90 ))
-- order by a.patid, a.fill_dt;

-- select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP_2
-- order by patid, fill_dt ;

create or replace table ac_rx_anti_dm_bas_bol_GLP_GIP_2 as
select distinct a.* from ty41_rx_anti_dm a
join (select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP_1 where rx_type='Basal') b on a.patid=b.patid
join (select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP_1 where rx_type="GLP1") c on b.patid=c.patid and b.drug_flag<>c.drug_flag
join (select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP_1 where rx_type='Bolus') d on c.patid=d.patid and c.drug_flag<>d.drug_flag
where 
((b.FILL_DT between c.FILL_DT and c.end_date + 90) OR
(c.FILL_DT between b.FILL_DT and b.end_date + 90 ) ) 
and
((c.FILL_DT between d.FILL_DT and d.end_date + 90) OR
(d.FILL_DT between c.FILL_DT and c.end_date + 90 ) )
and
((b.FILL_DT between d.FILL_DT and d.end_date + 90) OR
(d.FILL_DT between b.FILL_DT and b.end_date + 90 ) ) 
and a.rx_type in ('Basal','Bolus','GLP1') and a.FILL_DT between '2021-01-01' and '2021-12-31' 

order by a.patid, a.FILL_DT;

select distinct * from ac_rx_anti_dm_bas_bol_GLP_GIP_2
order by patid, FILL_DT ;

-- COMMAND ----------

-- select patid,count(distinct drug_flag) as n_drugs  from ac_rx_anti_dm_bas_bol_GLP_GIP_2
-- group by 1
-- order by 2 asc;

select distinct *  from ac_rx_anti_dm_bas_bol_GLP_GIP_2
where patid='33003292130'
order by patid, FILL_DT;

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
