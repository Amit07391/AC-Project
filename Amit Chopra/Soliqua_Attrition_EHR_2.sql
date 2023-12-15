-- Databricks notebook source
create or replace table ac_1206_ehr_sol_attr_final_table as
select distinct a.ptid, b.indx_a1c_dt from pat_ehr_all_sol a
inner join ac_ehr_pat_rx_basal_bolus_index b on a.ptid=b.ptid where isnotnull(dt_1st_t2dm) and isnotnull(dt_1st_Basal) and isnotnull(dt_1st_Bolus) and isnotnull(ptid1) and isnotnull(dt_last_bas_bol_bl) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_sol_bl);

select distinct ptid from ac_1206_ehr_sol_attr_final_table;

-- COMMAND ----------

select count(distinct ptid) from ac_1206_ehr_sol_attr_final_table;

-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_test as
select distinct a.*, b.indx_a1c_dt from ac_rx_anti_dm a
inner join ac_1206_ehr_sol_attr_final_table b on a.ptid=b.ptid
where a.rxdate>=b.indx_a1c_dt 
-- and a.rxdate<=b.indx_a1c_dt + 180 
order by a.ptid, a.rxdate;

select distinct * from ac_1206_ehr_sol_rx_test
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_1206_ehr_sol_rx_test
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

create or replace table ac_1206_ehr_sol_rx_attr_1 as
select distinct a.*, b.indx_a1c_dt from ac_rx_anti_dm a
inner join ac_1206_ehr_sol_attr_final_table b on a.ptid=b.ptid
where a.rxdate>=b.indx_a1c_dt 
and a.rxdate<=b.indx_a1c_dt + 180 
order by a.ptid, a.rxdate;

select distinct * from ac_1206_ehr_sol_rx_attr_1
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_1206_ehr_sol_rx_attr_1
where lower(brnd_nm) like '%soliqua%';


-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_2 as
select distinct *, 
case when rx_type in ('Basal','Bolus') then 'Basal/Bolus'
else rx_type end as flag
from ac_1206_ehr_sol_rx_attr_1
order by ptid, rxdate;

select distinct * from ac_1206_ehr_sol_rx_attr_2
order by ptid, rxdate;


-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_cnt as
select distinct ptid, count(distinct flag) as flg from ac_1206_ehr_sol_rx_attr_2
group by 1
order by 1;

select distinct * from ac_1206_ehr_sol_rx_attr_cnt

order by ptid;

-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_3 as
select distinct a.*, b.flg,
case when lcase(brnd_nm) like '%soliqua%' then 'Soliqua'
else flag end as rx_type2
from ac_1206_ehr_sol_rx_attr_2 a
left join ac_1206_ehr_sol_rx_attr_cnt b on a.ptid=b.ptid
order by a.ptid, a.rxdate;

select distinct * from ac_1206_ehr_sol_rx_attr_3
-- where patid ='33030051817'
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_1206_ehr_sol_rx_attr_3


-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_4 as
select distinct * from ac_1206_ehr_sol_rx_attr_wo_GLP
-- where flg<2 and 
where flag='Basal/Bolus'
order by ptid, rxdate;

select distinct * from ac_1206_ehr_sol_rx_attr_4
order by ptid, rxdate;


-- COMMAND ----------

create or replace table ac1206_ehr_bas_fu_use as
select distinct ptid, rxdate, rx_type, brnd_nm from
ac_1206_ehr_sol_rx_attr_4
where rx_type='Basal'
order by 1,2;

select distinct * from ac1206_ehr_bas_fu_use
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac1206_ehr_bol_fu_use as
select distinct ptid, rxdate, brnd_nm  from
ac_1206_ehr_sol_rx_attr_4
where rx_type='Bolus'
order by 1,2;

select distinct * from ac1206_ehr_bol_fu_use
order by 1,2;

-- COMMAND ----------

create or replace table ac1206_ehr_bol_fu_use_1 as
select distinct ptid, rxdate,brnd_nm,  'Bolus' as rx_type, lead(rxdate) over(partition by ptid order by rxdate) as Next_dt from
ac1206_ehr_bol_fu_use
order by 1,2;

select distinct * from ac1206_ehr_bol_fu_use_1
order by 1,2;

-- COMMAND ----------

create or replace table ac1206_bas_bol_fu_use as
select distinct a.*, b.rxdate as rx_dt_bas, b.brnd_nm as bas_brnd_nm  from ac1206_ehr_bol_fu_use_1 a
join ac1206_ehr_bas_fu_use b on a.ptid=b.ptid
where b.rxdate between a.rxdate and a.next_dt
order by a.ptid, a.rxdate;

select distinct * from ac1206_bas_bol_fu_use
-- where ptid='PT078158621'
order by ptid, rxdate;

-- COMMAND ----------

select distinct * from ac_1206_ehr_sol_rx_attr_4
where ptid='PT078208661'
order by rxdate;

-- COMMAND ----------

select count(distinct ptid ) from ac1206_bas_bol_fu_use

-- COMMAND ----------

-- MAGIC %md #### Patients who remained on BB, but no GLP1

-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_GLP as
select distinct * from ac_1206_ehr_sol_rx_attr_3 
where rx_type='GLP1'
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_wo_GLP as
select distinct * from ac_1206_ehr_sol_rx_attr_3
where ptid not in (select distinct ptid from ac_1206_ehr_sol_rx_attr_GLP )
order by ptid, rxdate;

select distinct * from ac_1206_ehr_sol_rx_attr_wo_GLP
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_rank as
select distinct *
from (select distinct *, dense_rank() OVER (PARTITION BY ptid ORDER BY rxdate) as rank
from (select distinct ptid, rxdate, rx_type2 from ac_1206_ehr_sol_rx_attr_wo_GLP))
order by ptid
;

select distinct * from ac_1206_ehr_sol_rx_attr_rank
order by ptid, rxdate, rank;



-- COMMAND ----------

-- MAGIC %md #### Patients with soliqua as their first treatment after index

-- COMMAND ----------

select distinct * from ac_1206_ehr_sol_rx_attr_3
where ptid='PT079094788'
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_1206_ehr_rx_fst_trt_aftr_indx_sol as
select distinct * from ac_1206_ehr_sol_rx_attr_rank
where  rx_type2='Soliqua' 
and rank<2 
order by ptid, rxdate;

select distinct * from ac_1206_ehr_rx_fst_trt_aftr_indx_sol
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_1206_ehr_rx_fst_trt_aftr_indx_sol

-- COMMAND ----------

-- MAGIC %md #### patients who have soliqua as their first trmnt but don't have BB

-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_sol_no_BB as
select distinct * from ac_1206_ehr_rx_fst_trt_aftr_indx_sol
where ptid not in (select distinct ptid from ac1206_bas_bol_fu_use)
order by ptid, rxdate;

select distinct * from ac_1206_ehr_sol_rx_attr_sol_no_BB
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_1206_ehr_sol_rx_attr_sol_no_BB;

-- select distinct * from ac_1206_sol_rx_attr_wo_GLP
-- where patid='33089849796'
-- order by patid, fill_dt;

-- COMMAND ----------

-- MAGIC %md #### patients who started with soliqua and remained on BB but no GLP1

-- COMMAND ----------

create or replace table ac_1206_ehr_sol_rx_attr_sol_with_BB as
select distinct a.* from ac_1206_ehr_rx_fst_trt_aftr_indx_sol a
inner join ac1206_bas_bol_fu_use b on a.ptid=b.ptid
order by a.ptid, a.rxdate;

select distinct * from ac_1206_ehr_sol_rx_attr_sol_with_BB
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_1206_ehr_sol_rx_attr_sol_with_BB;

-- create or replace table ac_1206_ehr_sol_rx_attr_rank_all as
-- select distinct *
-- from (select distinct *, dense_rank() OVER (PARTITION BY ptid ORDER BY rxdate) as rank
-- from (select distinct ptid, rxdate, rx_type2 from ac_1206_ehr_sol_rx_attr_3))
-- order by ptid
-- ;

-- select distinct * from ac_1206_ehr_sol_rx_attr_rank_all
-- order by ptid, rxdate, rank;


select distinct rx_type2, count(distinct ptid) from ac_1206_ehr_sol_rx_attr_rank_all
where rank<2 
group by 1
order by 2 desc;

-- COMMAND ----------



-- COMMAND ----------

select distinct ahfsclss_desc, rx_type from ty00_ses_rx_anti_dm_loopup
