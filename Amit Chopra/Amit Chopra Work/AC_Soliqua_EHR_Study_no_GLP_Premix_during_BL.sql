-- Databricks notebook source
select distinct rx_type from ac_ehr_sol_rx_anti_dm_bl

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_glp_prmx_bl as
select distinct * from ac_ehr_sol_rx_anti_dm_bl
where rx_type in ('GLP1','PreMix')
order by ptid, rxdate;

select distinct * from ac_ehr_sol_rx_glp_prmx_bl
order by ptid, rxdate;

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_no_glp_prmx_bl as
select distinct * from ac_ehr_sol_rx_anti_dm_bl
where ptid not in (select distinct ptid from ac_ehr_sol_rx_glp_prmx_bl)
order by ptid, rxdate;

select distinct * from ac_ehr_sol_rx_no_glp_prmx_bl
order by ptid, rxdate;

-- COMMAND ----------

select distinct rx_type from ac_ehr_sol_rx_no_glp_prmx_bl

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_no_glp_prmx_bl

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_hba1c_no_glp_prmx as
select distinct a.ptid, a.dt_rx_index, b.bl_hba1c_val, b.fl_hba1c_val, c.age_index, c.GENDER  from ac_ehr_sol_rx_no_glp_prmx_bl a
inner join ac_ehr_sol_lab_a1c_sol_bl_fl b on a.ptid=b.ptid
left join ac_ehr_sol_study_final_pts c on a.ptid=c.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_rx_hba1c_no_glp_prmx
order by ptid;

-- COMMAND ----------

select mean(bl_hba1c_val) as mn, median(bl_hba1c_val) as med, std(bl_hba1c_val) as std, min(bl_hba1c_val) as min
        , percentile(bl_hba1c_val,.25) as p25, percentile(bl_hba1c_val,.5) as median, percentile(bl_hba1c_val,.75) as p75, max(bl_hba1c_val) as max  from ac_ehr_sol_rx_hba1c_no_glp_prmx;

        select mean(fl_hba1c_val) as mn, median(fl_hba1c_val) as med, std(fl_hba1c_val) as std, min(fl_hba1c_val) as min
        , percentile(fl_hba1c_val,.25) as p25, percentile(fl_hba1c_val,.5) as median, percentile(fl_hba1c_val,.75) as p75, max(fl_hba1c_val) as max  from ac_ehr_sol_rx_hba1c_no_glp_prmx;

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_hba1c_val - fl_hba1c_val as diff from ac_ehr_sol_rx_hba1c_no_glp_prmx)

-- COMMAND ----------

create or replace table ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl as
select distinct a.ptid, a.value1, b.bl_hba1c_val from ac_ehr_sol_lab_a1c_sol_30dys_fu_val a
left join ac_ehr_sol_lab_a1c_sol_bl_fl b on a.ptid=b.ptid
inner join ac_ehr_sol_rx_no_glp_prmx_bl c on a.ptid=c.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl
order by ptid;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl

-- COMMAND ----------

select mean(bl_hba1c_val) as mn, median(bl_hba1c_val) as med, std(bl_hba1c_val) as std, min(bl_hba1c_val) as min
        , percentile(bl_hba1c_val,.25) as p25, percentile(bl_hba1c_val,.5) as median, percentile(bl_hba1c_val,.75) as p75, max(bl_hba1c_val) as max  from ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl;

        select mean(value1) as mn, median(value1) as med, std(value1) as std, min(value1) as min
        , percentile(value1,.25) as p25, percentile(value1,.5) as median, percentile(value1,.75) as p75, max(value1) as max  from ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl;

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_hba1c_val - value1 as diff from ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl)

-- COMMAND ----------



-- select count(distinct ptid) from ac_ehr_sol_rx_hba1c_no_glp_prmx
-- where bl_hba1c_val<7;

-- select count(distinct ptid) from ac_ehr_sol_rx_hba1c_no_glp_prmx
-- where fl_hba1c_val<7;

-- select count(distinct ptid) from ac_ehr_sol_rx_hba1c_no_glp_prmx
-- where bl_hba1c_val<8;

-- select count(distinct ptid) from ac_ehr_sol_rx_hba1c_no_glp_prmx
-- where fl_hba1c_val<8;

select count(distinct ptid) from ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl
where value1<7;

select count(distinct ptid) from ac_ehr_sol_a1c_30dys_no_glp_prmx_bl_fl
where value1<8;


-- COMMAND ----------

create or replace table ac_ehr_sol_WT_no_GLP1_prmx as
select distinct a.* from ac_ehr_sol_WT_sol_bl_fu a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_WT_no_GLP1_prmx
order by ptid;

-- COMMAND ----------

select distinct * from ac_ehr_sol_WT_no_GLP1_prmx
where bl_wt_val is not null and fl_wt_val is not null

-- COMMAND ----------

select mean(bl_wt_val) as mn, median(bl_wt_val) as med, std(bl_wt_val) as std, min(bl_wt_val) as min
        , percentile(bl_wt_val,.25) as p25, percentile(bl_wt_val,.5) as median, percentile(bl_wt_val,.75) as p75, max(bl_wt_val) as max  from ac_ehr_sol_WT_no_GLP1_prmx
        where bl_wt_val is not null and fl_wt_val is not null;

        select mean(fl_wt_val) as mn, median(fl_wt_val) as med, std(fl_wt_val) as std, min(fl_wt_val) as min
        , percentile(fl_wt_val,.25) as p25, percentile(fl_wt_val,.5) as median, percentile(fl_wt_val,.75) as p75, max(fl_wt_val) as max  from ac_ehr_sol_WT_no_GLP1_prmx
        where bl_wt_val is not null and fl_wt_val is not null;

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_wt_val - fl_wt_val as diff from ac_ehr_sol_WT_no_GLP1_prmx)

-- COMMAND ----------

create or replace table ac_ehr_sol_BMI_no_GLP1_prmx as
select distinct a.* from ac_ehr_sol_BMI_sol_bl_fu a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_BMI_no_GLP1_prmx
order by ptid;

-- COMMAND ----------

select distinct * from ac_ehr_sol_BMI_no_GLP1_prmx
where bl_BMI_val is not null and fl_BMI_val is not null

-- COMMAND ----------

select mean(bl_BMI_val) as mn, median(bl_BMI_val) as med, std(bl_BMI_val) as std, min(bl_BMI_val) as min
        , percentile(bl_BMI_val,.25) as p25, percentile(bl_BMI_val,.5) as median, percentile(bl_BMI_val,.75) as p75, max(bl_BMI_val) as max  from ac_ehr_sol_BMI_no_GLP1_prmx
        where bl_BMI_val is not null and fl_BMI_val is not null;

        select mean(fl_BMI_val) as mn, median(fl_BMI_val) as med, std(fl_BMI_val) as std, min(fl_BMI_val) as min
        , percentile(fl_BMI_val,.25) as p25, percentile(fl_BMI_val,.5) as median, percentile(fl_BMI_val,.75) as p75, max(fl_BMI_val) as max  from ac_ehr_sol_BMI_no_GLP1_prmx
        where bl_BMI_val is not null and fl_BMI_val is not null;

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_BMI_val - fl_BMI_val as diff from ac_ehr_sol_BMI_no_GLP1_prmx
         where bl_BMI_val is not null and fl_BMI_val is not null)

-- COMMAND ----------


select count(distinct a.ptid) from ac_ehr_soliqua_hypo_lab_dx_bl a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;


select count(distinct a.ptid) from ac_ehr_soliqua_hypo_lab_dx_fu a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;

select count(distinct a.DIAG_DATE) from ac_ehr_soliqua_hypo_lab_dx_bl a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;

select count(distinct a.DIAG_DATE) from ac_ehr_soliqua_hypo_lab_dx_fu a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_2

-- COMMAND ----------

-- create or replace table ac_ehr_sol_OAD_use_no_glp_prmx_bl as
-- select distinct a.ptid, count(distinct a.rx_type) as cnt from ac_ehr_sol_rx_anti_dm_bl_2 a
-- inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
-- where drug_cat='OAD'
-- group by a.ptid
-- order by a.ptid;

-- select distinct * from ac_ehr_sol_OAD_use_no_glp_prmx_bl
-- order by ptid;

-- select count(distinct ptid) from ac_ehr_sol_OAD_use_no_glp_prmx_bl
-- where cnt=1;

-- select count(distinct ptid) from ac_ehr_sol_OAD_use_no_glp_prmx_bl
;

select a.rx_type, count(distinct a.ptid) from ac_ehr_sol_rx_anti_dm_bl_2 a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
-- where drug_cat='OAD' 
group by 1
order by 1 ;

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_bl_no_glp_prmx_basal as
select distinct a.*,
case when lcase(a.brnd_nm) like '%toujeo%' then 'Toujeo'
when lcase(a.gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(a.brnd_nm) not like '%toujeo%' then 'Gla-100'
when lcase(a.gnrc_nm) like '%detemir%' then 'Detemir'
when lcase(a.gnrc_nm) like '%insulin degludec%' and lcase(a.brnd_nm) like '%tresiba%' then 'DEGLUDEC'
else a.rx_type end as rx_type2
from ac_ehr_sol_rx_anti_dm_bl_2 a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
where a.rx_type='Basal'
order by a.ptid, a.rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_bl_no_glp_prmx_basal
order by ptid, rxdate;

-- COMMAND ----------

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_no_glp_prmx_basal
group by 1
order by 1;

-- COMMAND ----------

create or replace table ac_ehr_sol_rx_anti_dm_bl_no_glp_prmx_bolus as
select distinct a.*,
case when a.gnrc_nm like '%INSULIN REGULAR%' then 'INSULIN REGULAR'
else a.gnrc_nm end as rx_type2
from ac_ehr_sol_rx_anti_dm_bl_2 a
inner join ac_ehr_sol_rx_no_glp_prmx_bl b on a.ptid=b.ptid
where a.rx_type='Bolus'
order by a.ptid, a.rxdate;

select distinct * from ac_ehr_sol_rx_anti_dm_bl_no_glp_prmx_bolus
order by ptid, rxdate;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_no_glp_prmx_bolus

-- COMMAND ----------

select rx_type2, count(distinct ptid) from ac_ehr_sol_rx_anti_dm_bl_no_glp_prmx_bolus
group by 1
order by 1;
