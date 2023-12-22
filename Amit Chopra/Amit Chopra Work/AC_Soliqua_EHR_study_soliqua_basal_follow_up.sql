-- Databricks notebook source
create or replace table ac_ehr_sol_rx_anti_dm_fu as
select distinct a.*, b.dt_rx_index from ac_rx_anti_dm_16_23 a
join ac_ehr_sol_study_final_pts b
on a.ptid=b.ptid
where a.rxdate between dt_rx_index + 1 and dt_rx_index + 180
order by a.ptid, rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_fu
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_fu_2 as
select distinct *, 
case when lower(rx_type) like '%metformin%' or lower(rx_type) like '%sglt%' or rx_type= 'AGI' or rx_type='DPP-4'
or rx_type ='TZD' or rx_type = 'Meglitinide' or rx_type = 'Sulfonylureas' then 'OAD'
else rx_type end as Drug_cat
from ac_ehr_sol_rx_anti_dm_fu
order by ptid, rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_fu_2
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_soliqua_demo_hba1c_bl_pts_2

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_fu_demo as
select distinct a.*, b.gender, b.age_index, b.value1  from ac_ehr_sol_rx_anti_dm_fu_2 a
left join ac_ehr_soliqua_demo_hba1c_bl_pts_2 b on a.ptid=b.ptid
order by a.ptid, a.rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_fu_demo
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_fu_basal as
select distinct *,
case when lcase(brnd_nm) like '%toujeo%' then 'Toujeo'
when lcase(gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(brnd_nm) not like '%toujeo%' then 'Gla-100'
when lcase(gnrc_nm) like '%detemir%' then 'Detemir'
when lcase(gnrc_nm) like '%insulin degludec%' and lcase(brnd_nm) like '%tresiba%' then 'DEGLUDEC'
else rx_type end as rx_type2
from ac_ehr_sol_rx_anti_dm_fu_demo
where rx_type='Basal'
order by ptid, rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_fu_basal
order by ptid, rxdate;

-- COMMAND ----------

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_fu_basal
group by 1
order by 1;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_fu_basal

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_fu_basal
where age_index>=65;

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_fu_basal
where value1>=7;

-- COMMAND ----------

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_fu_basal
where age_index>=65
group by 1
order by 1;

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_fu_basal
where value1>=7
group by 1
order by 1;


-- COMMAND ----------

select count(distinct a.ptid) from ac_ehr_sol_rx_anti_dm_fu_basal a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;

select a.rx_type2, count(distinct a.ptid) from ac_ehr_sol_rx_anti_dm_fu_basal a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
group by 1
order by 1;

-- COMMAND ----------

select count(distinct a.ptid) from ac_ehr_sol_rx_anti_dm_fu_basal a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;

select a.rx_type2, count(distinct a.ptid) from ac_ehr_sol_rx_anti_dm_fu_basal a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
group by 1
order by 1;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_rx_sol_bol_fu_final

-- COMMAND ----------

select count(distinct a.ptid) from ac_ehr_sol_rx_anti_dm_fu_basal a
inner join ac_ehr_rx_sol_bol_fu_final b on a.ptid=b.ptid;

select a.rx_type2, count(distinct a.ptid) from ac_ehr_sol_rx_anti_dm_fu_basal a
inner join ac_ehr_rx_sol_bol_fu_final b on a.ptid=b.ptid
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_rx_anti_dm_soliqua_fu as
select distinct * from ac_ehr_sol_rx_anti_dm_fu_demo
where lower(BRND_NM) like '%soliqua%'
order by ptid, rxdate;

select distinct * from ac_ehr_rx_anti_dm_soliqua_fu
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_ehr_rx_anti_dm_soliqua_fu_cnt as
select distinct ptid, count(distinct rxdate) as rx_cnt from ac_ehr_rx_anti_dm_soliqua_fu
group by ptid
order by ptid;

select distinct * from ac_ehr_rx_anti_dm_soliqua_fu_cnt
order by ptid;

-- COMMAND ----------

select mean(rx_cnt) as mn, median(rx_cnt) as med, std(rx_cnt) as std, min(rx_cnt) as min
        , percentile(rx_cnt,.25) as p25, percentile(rx_cnt,.5) as median, percentile(rx_cnt,.75) as p75, max(rx_cnt) as max  from ac_ehr_rx_anti_dm_soliqua_fu_cnt;

        select mean(rx_cnt) as mn, median(rx_cnt) as med, std(rx_cnt) as std, min(rx_cnt) as min
        , percentile(rx_cnt,.25) as p25, percentile(rx_cnt,.5) as median, percentile(rx_cnt,.75) as p75, max(rx_cnt) as max  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        left join ac_ehr_rx_anti_dm_soliqua_fu b on a.ptid=b.ptid
        where b.age_index>=65;


    select mean(rx_cnt) as mn, median(rx_cnt) as med, std(rx_cnt) as std, min(rx_cnt) as min
        , percentile(rx_cnt,.25) as p25, percentile(rx_cnt,.5) as median, percentile(rx_cnt,.75) as p75, max(rx_cnt) as max  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        left join ac_ehr_rx_anti_dm_soliqua_fu b on a.ptid=b.ptid
        where b.value1>=7;

-- COMMAND ----------

select count(distinct ptid)  from ac_ehr_rx_anti_dm_soliqua_fu_cnt;

  select count(distinct a.ptid)  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        left join ac_ehr_rx_anti_dm_soliqua_fu b on a.ptid=b.ptid
        where b.age_index>=65;

        select count(distinct a.ptid)  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        left join ac_ehr_rx_anti_dm_soliqua_fu b on a.ptid=b.ptid
        where b.value1>=7;

-- COMMAND ----------

     select mean(rx_cnt) as mn, median(rx_cnt) as med, std(rx_cnt) as std, min(rx_cnt) as min
        , percentile(rx_cnt,.25) as p25, percentile(rx_cnt,.5) as median, percentile(rx_cnt,.75) as p75, max(rx_cnt) as max  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;

          select count(distinct a.ptid)  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;

-- COMMAND ----------

    select mean(rx_cnt) as mn, median(rx_cnt) as med, std(rx_cnt) as std, min(rx_cnt) as min
        , percentile(rx_cnt,.25) as p25, percentile(rx_cnt,.5) as median, percentile(rx_cnt,.75) as p75, max(rx_cnt) as max  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        inner join ac_ehr_rx_sol_bol_fu_final b on a.ptid=b.ptid;


          select count(distinct a.ptid)  from ac_ehr_rx_anti_dm_soliqua_fu_cnt a
        inner join ac_ehr_rx_sol_bol_fu_final b on a.ptid=b.ptid;
