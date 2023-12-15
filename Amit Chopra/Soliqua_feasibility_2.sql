-- Databricks notebook source
create or replace table ac_1206_sol_attr_final_table as
select distinct a.patid, b.indx_a1c_dt from pat_all_sol a
inner join ac_pat_rx_basal_bolus_index b on a.patid=b.patid where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(patid1) and isnotnull(dt_last_bas_bol_bl) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_sol_bl);

select distinct patid from ac_1206_sol_attr_final_table;

-- COMMAND ----------

select count(distinct patid) from ac_1206_sol_attr_final_table;

-- COMMAND ----------

create or replace table ac_1206_sol_rx_test as
select distinct a.*, b.indx_a1c_dt from ac_2305_rx_anti_dm a
inner join ac_1206_sol_attr_final_table b on a.patid=b.patid
where a.fill_dt>=b.indx_a1c_dt 
-- and a.fill_dt<=b.indx_a1c_dt + 180 
order by a.patid, a.FILL_DT;

select distinct * from ac_1206_sol_rx_test
order by patid, FILL_DT;

-- COMMAND ----------

select count(distinct patid) from ac_1206_sol_rx_test
where lower(brnd_nm) like '%soliqua%';

-- COMMAND ----------

-- create or replace table ac_1206_sol_rx_attr_1 as
-- select distinct a.*, b.indx_a1c_dt from ac_2305_rx_anti_dm a
-- inner join ac_1206_sol_attr_final_table b on a.patid=b.patid
-- where a.fill_dt>=b.indx_a1c_dt 
-- and a.fill_dt<=b.indx_a1c_dt + 366 
-- order by a.patid, a.FILL_DT;

-- select distinct * from ac_1206_sol_rx_attr_1
-- order by patid, FILL_DT;

create or replace table ac_1206_sol_rx_attr_1 as
select distinct a.*, b.indx_a1c_dt from ac_2305_rx_anti_dm a
inner join ac_1206_sol_attr_final_table b on a.patid=b.patid
where a.fill_dt>=b.indx_a1c_dt 
and a.fill_dt<=b.indx_a1c_dt + 180 
order by a.patid, a.FILL_DT;

select distinct * from ac_1206_sol_rx_attr_1
order by patid, FILL_DT;



-- COMMAND ----------

select count(distinct patid) from ac_1206_sol_rx_attr_1
where lower(brnd_nm) like '%soliqua%';

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_2 as
select distinct *, 
case when rx_type in ('Basal','Bolus') then 'Basal/Bolus'
else rx_type end as flag
from ac_1206_sol_rx_attr_1
order by patid, fill_dt;

select distinct * from ac_1206_sol_rx_attr_2
order by patid, fill_dt;


-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_cnt as
select distinct patid, count(distinct flag) as flg from ac_1206_sol_rx_attr_2
group by 1
order by 1;

select distinct * from ac_1206_sol_rx_attr_cnt

order by patid;

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_3 as
select distinct a.*, b.flg,
case when lcase(brnd_nm) like '%soliqua%' then 'Soliqua'
else flag end as rx_type2
from ac_1206_sol_rx_attr_2 a
left join ac_1206_sol_rx_attr_cnt b on a.patid=b.patid
order by patid, fill_dt;

select distinct * from ac_1206_sol_rx_attr_3
-- where patid ='33030051817'
order by patid, fill_dt;

-- COMMAND ----------

select count(distinct patid) from ac_1206_sol_rx_attr_1

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_4 as
select distinct *, dateadd(fill_dt, cast(days_sup as int)) as end_date from ac_1206_sol_rx_attr_wo_GLP
-- where flg<2 and 
where flag='Basal/Bolus'
order by patid, fill_dt;

select distinct * from ac_1206_sol_rx_attr_4
order by patid, fill_dt;


-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_bb as
select distinct a.* from ac_1206_sol_rx_attr_4 a 
join ac_1206_sol_rx_attr_4 b on a.patid=b.patid and a.rx_type<>b.rx_type
where ((date_diff(b.end_date,a.fill_dt)>=30 ) AND
(date_diff(a.end_date,b.fill_dt)>=30 ))
order by a.patid, a.fill_dt;

select distinct * from ac_1206_sol_rx_attr_bb
order by patid, fill_dt;

-- COMMAND ----------

select count(distinct patid ) from ac_1206_sol_rx_attr_bb

-- COMMAND ----------

select distinct * from ac_1206_sol_rx_attr_3
where rx_type2='Soliqua'
order by patid, fill_dt;

-- COMMAND ----------

select distinct *, dateadd(fill_dt, cast(days_sup as int)) as end_date from ac_1206_sol_rx_attr_3
where patid='33085555180'
order by patid, fill_dt

-- COMMAND ----------

-- MAGIC %md #### Patients who remained on BB, but no GLP1

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_GLP as
select distinct * from ac_1206_sol_rx_attr_3 
where rx_type='GLP1'
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_wo_GLP as
select distinct * from ac_1206_sol_rx_attr_3
where patid not in (select distinct patid from ac_1206_sol_rx_attr_GLP )
order by patid, fill_dt;

select distinct * from ac_1206_sol_rx_attr_wo_GLP
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_rank as
select distinct *
from (select distinct *, dense_rank() OVER (PARTITION BY patid ORDER BY fill_dt) as rank
from (select distinct patid, fill_dt, rx_type2 from ac_1206_sol_rx_attr_wo_GLP))
order by patid
;

select distinct * from ac_1206_sol_rx_attr_rank
order by patid, fill_dt, rank;



-- COMMAND ----------

-- MAGIC %md #### Patients with soliqua as their first treatment after index

-- COMMAND ----------

select distinct * from ac_1206_sol_rx_attr_3
where patid='33015072042'
order by patid, fill_dt;

-- COMMAND ----------

create or replace table ac_1206_rx_fst_trt_aftr_indx_sol as
select distinct * from ac_1206_sol_rx_attr_rank
where  rx_type2='Soliqua' 
-- and rank<2 
order by patid, fill_dt;

select distinct * from ac_1206_rx_fst_trt_aftr_indx_sol
order by patid, fill_dt;

-- COMMAND ----------

select count(distinct patid) from ac_1206_rx_fst_trt_aftr_indx_sol

-- COMMAND ----------

-- MAGIC %md #### patients who have soliqua as their first trmnt but don't have BB

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_sol_no_BB as
select distinct * from ac_1206_rx_fst_trt_aftr_indx_sol
where patid not in (select distinct patid from ac_1206_sol_rx_attr_bb)
order by patid, fill_dt;

select distinct * from ac_1206_sol_rx_attr_sol_no_BB
order by patid, fill_dt;

-- COMMAND ----------

select count(distinct patid) from ac_1206_sol_rx_attr_sol_no_BB;

-- select distinct * from ac_1206_sol_rx_attr_wo_GLP
-- where patid='33089849796'
-- order by patid, fill_dt;

-- COMMAND ----------

-- MAGIC %md #### patients who started with soliqua and remained on BB but no GLP1

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_sol_with_BB as
select distinct a.* from ac_1206_rx_fst_trt_aftr_indx_sol a
inner join ac_1206_sol_rx_attr_bb b on a.patid=b.patid
order by patid, fill_dt;

select distinct * from ac_1206_sol_rx_attr_sol_with_BB
order by patid, fill_dt;

-- COMMAND ----------

select count(distinct patid) from ac_1206_sol_rx_attr_sol_with_BB;

-- create or replace table ac_1206_sol_rx_attr_rank_all as
-- select distinct *
-- from (select distinct *, dense_rank() OVER (PARTITION BY patid ORDER BY fill_dt) as rank
-- from (select distinct patid, fill_dt, rx_type2 from ac_1206_sol_rx_attr_3))
-- order by patid
-- ;

-- select distinct * from ac_1206_sol_rx_attr_rank_all
-- order by patid, fill_dt, rank;


-- select distinct rx_type2, count(distinct patid) from ac_1206_sol_rx_attr_rank_all
-- where rank<2 
-- group by 1
-- order by 2 desc;

-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_4 as
select distinct *, dateadd(fill_dt, cast(days_sup as int)) as end_date from ac_1206_sol_rx_attr_wo_GLP
-- where flg<2 and 
where flag='Basal/Bolus'
order by patid, fill_dt;

select distinct * from ac_1206_sol_rx_attr_4
order by patid, fill_dt;


-- COMMAND ----------




select distinct * from ac_1206_sol_rx_attr_bb
order by patid, fill_dt;

select count(distinct patid) from ac_1206_sol_rx_attr_4


-- COMMAND ----------

create or replace table ac_1206_sol_rx_attr_bb as
select distinct a.* from ac_1206_sol_rx_attr_4 a 
join ac_1206_sol_rx_attr_4 b on a.patid=b.patid and a.rx_type<>b.rx_type
where ((date_diff(b.end_date,a.fill_dt)>=30 ) AND
(date_diff(a.end_date,b.fill_dt)>=30 ))
order by a.patid, a.fill_dt;

select distinct * from ac_1206_sol_rx_attr_bb
order by patid, fill_dt;

-- COMMAND ----------

select count(distinct patid) from ac_1206_sol_rx_attr_bb

-- COMMAND ----------

-- MAGIC %md #### started and switched to Soliqua but not on GLP1 and BB

-- COMMAND ----------



-- COMMAND ----------

select distinct ahfsclss_desc, rx_type from ty00_ses_rx_anti_dm_loopup
