-- Databricks notebook source
select distinct * from ac_ehr_sol_lab_a1c_sol_bl_fl;
select distinct * from ac_ehr_sol_study_final_pts; 


-- COMMAND ----------

create or replace table ac_ehr_sol_study_final_a1c_bl_fl as
select distinct a.*, b.dt_rx_index, b.age_index, b.GENDER,c.value1 from ac_ehr_sol_lab_a1c_sol_bl_fl a 
left join ac_ehr_sol_study_final_pts b on a.ptid=b.ptid
left join ac_ehr_sol_lab_a1c_sol_bl_val c on a.ptid=c.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_study_final_a1c_bl_fl
order by ptid;

-- COMMAND ----------

create or replace table ac_ehr_sol_study_final_a1c_7plus as
select distinct * from ac_ehr_sol_study_final_a1c_bl_fl
where bl_hba1c_val>=7
order by ptid;

select distinct * from ac_ehr_sol_study_final_a1c_7plus
order by ptid;

-- COMMAND ----------

select mean(bl_hba1c_val) as mn, median(bl_hba1c_val) as med, std(bl_hba1c_val) as std, min(bl_hba1c_val) as min
        , percentile(bl_hba1c_val,.25) as p25, percentile(bl_hba1c_val,.5) as median, percentile(bl_hba1c_val,.75) as p75, max(bl_hba1c_val) as max  from ac_ehr_sol_study_final_a1c_7plus;

        select mean(fl_hba1c_val) as mn, median(fl_hba1c_val) as med, std(fl_hba1c_val) as std, min(fl_hba1c_val) as min
        , percentile(fl_hba1c_val,.25) as p25, percentile(fl_hba1c_val,.5) as median, percentile(fl_hba1c_val,.75) as p75, max(fl_hba1c_val) as max  from ac_ehr_sol_study_final_a1c_7plus;

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_hba1c_val - fl_hba1c_val as diff from ac_ehr_sol_study_final_a1c_7plus)

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_study_final_a1c_7plus
where bl_hba1c_val<7;

select count(distinct ptid) from ac_ehr_sol_study_final_a1c_7plus
where fl_hba1c_val<7;

select count(distinct ptid) from ac_ehr_sol_study_final_a1c_7plus
where bl_hba1c_val<8;

select count(distinct ptid) from ac_ehr_sol_study_final_a1c_7plus
where fl_hba1c_val<8;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_WT_sol_bl_val; --367

select count(distinct ptid) from ac_ehr_sol_WT_sol_fu_val; --361

-- COMMAND ----------

create or replace table ac_ehr_sol_study_final_a1c_30dys_fu as
select distinct a.*, b.dt_rx_index, b.age_index, b.GENDER,c.value1 as bl_hba1c_val from ac_ehr_sol_lab_a1c_sol_30dys_fu_val a 
left join ac_ehr_sol_study_final_pts b on a.ptid=b.ptid
left join ac_ehr_sol_lab_a1c_sol_bl_val c on a.ptid=c.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_study_final_a1c_30dys_fu
order by ptid; 

-- COMMAND ----------

select distinct * from ac_ehr_sol_study_final_a1c_30dys_fu
where bl_hba1c_val>=7

-- COMMAND ----------

  select mean(value1) as mn, median(value1) as med, std(value1) as std, min(value1) as min
        , percentile(value1,.25) as p25, percentile(value1,.5) as median, percentile(value1,.75) as p75, max(bl_hba1c_val) as max  from ac_ehr_sol_study_final_a1c_30dys_fu
        where bl_hba1c_val>=7;

  select mean(bl_hba1c_val) as mn, median(bl_hba1c_val) as med, std(bl_hba1c_val) as std, min(bl_hba1c_val) as min
        , percentile(bl_hba1c_val,.25) as p25, percentile(bl_hba1c_val,.5) as median, percentile(bl_hba1c_val,.75) as p75, max(bl_hba1c_val) as max  from ac_ehr_sol_study_final_a1c_30dys_fu
        where bl_hba1c_val>=7
        ;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_study_final_a1c_30dys_fu
where bl_hba1c_val>=7;


 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_hba1c_val - value1 as diff from ac_ehr_sol_study_final_a1c_30dys_fu
        where bl_hba1c_val>=7);

-- COMMAND ----------

select distinct * from ac_ehr_sol_study_final_a1c_30dys_fu
where bl_hba1c_val>=7;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_study_final_a1c_30dys_fu
where value1<7 and bl_hba1c_val>=7;

select count(distinct ptid) from ac_ehr_sol_study_final_a1c_30dys_fu
where value1<8 and bl_hba1c_val>=7;

-- COMMAND ----------

create or replace table ac_ehr_sol_study_final_WT_bl_fl as
select distinct a.*, b.dt_rx_index, b.age_index, b.GENDER,c.value1 from ac_ehr_sol_WT_sol_bl_fu a 
left join ac_ehr_sol_study_final_pts b on a.ptid=b.ptid
left join ac_ehr_sol_lab_a1c_sol_bl_val c on a.ptid=c.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_study_final_WT_bl_fl
order by ptid;

-- COMMAND ----------

create or replace table ac_ehr_sol_study_final_WT_a1c_7plus as
select distinct * from ac_ehr_sol_study_final_WT_bl_fl
where value1>=7;
select distinct * from ac_ehr_sol_study_final_WT_a1c_7plus
order by ptid;

-- COMMAND ----------

select distinct * from ac_ehr_sol_study_final_WT_a1c_7plus
where bl_wt_val is not null
 and fl_wt_val is not null 

-- COMMAND ----------

select mean(bl_wt_val) as mn, median(bl_wt_val) as med, std(bl_wt_val) as std, min(bl_wt_val) as min
        , percentile(bl_wt_val,.25) as p25, percentile(bl_wt_val,.5) as median, percentile(bl_wt_val,.75) as p75, max(bl_wt_val) as max  from ac_ehr_sol_study_final_WT_a1c_7plus
        where bl_wt_val is not null and fl_wt_val is not null;

        select mean(fl_wt_val) as mn, median(fl_wt_val) as med, std(fl_wt_val) as std, min(fl_wt_val) as min
        , percentile(fl_wt_val,.25) as p25, percentile(fl_wt_val,.5) as median, percentile(fl_wt_val,.75) as p75, max(fl_wt_val) as max  from ac_ehr_sol_study_final_WT_a1c_7plus
        where bl_wt_val is not null and fl_wt_val is not null;

-- COMMAND ----------



select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
, percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
from (select distinct ptid,  bl_wt_val - fl_wt_val as diff from ac_ehr_sol_study_final_WT_a1c_7plus
where bl_wt_val is not null and fl_wt_val is not null )

-- COMMAND ----------

select distinct ptid,  bl_wt_val - fl_wt_val as diff from ac_ehr_sol_WT_sol_bl_fu

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_BMI_sol_bl_val; --365

select count(distinct ptid) from ac_ehr_sol_BMI_sol_fu_val; --360

-- COMMAND ----------

create or replace table ac_ehr_sol_study_final_BMI_bl_fl as
select distinct a.*, b.dt_rx_index, b.age_index, b.GENDER,c.value1 from ac_ehr_sol_BMI_sol_bl_fu a 
left join ac_ehr_sol_study_final_pts b on a.ptid=b.ptid
left join ac_ehr_sol_lab_a1c_sol_bl_val c on a.ptid=c.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_study_final_BMI_bl_fl
order by ptid;

-- COMMAND ----------

create or replace table ac_ehr_sol_study_final_BMI_a1c_7plus as
select distinct * from ac_ehr_sol_study_final_BMI_bl_fl
where value1>=7;
select distinct * from ac_ehr_sol_study_final_BMI_a1c_7plus
order by ptid;

-- COMMAND ----------

select mean(bl_BMI_val) as mn, median(bl_BMI_val) as med, std(bl_BMI_val) as std, min(bl_BMI_val) as min
        , percentile(bl_BMI_val,.25) as p25, percentile(bl_BMI_val,.5) as median, percentile(bl_BMI_val,.75) as p75, max(bl_BMI_val) as max  from ac_ehr_sol_study_final_BMI_a1c_7plus
        where bl_BMI_val is not null and fl_BMI_val is not null;

        select mean(fl_BMI_val) as mn, median(fl_BMI_val) as med, std(fl_BMI_val) as std, min(fl_BMI_val) as min
        , percentile(fl_BMI_val,.25) as p25, percentile(fl_BMI_val,.5) as median, percentile(fl_BMI_val,.75) as p75, max(fl_BMI_val) as max  from ac_ehr_sol_study_final_BMI_a1c_7plus
        where bl_BMI_val is not null and fl_BMI_val is not null;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_study_final_BMI_a1c_7plus
where bl_BMI_val is not null 
and fl_BMI_val is not null ;

select distinct * from ac_ehr_sol_study_final_BMI_a1c_7plus
where bl_BMI_val is not null and fl_BMI_val is not null ;

-- COMMAND ----------



 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_BMI_val - fl_BMI_val as diff from ac_ehr_sol_study_final_BMI_a1c_7plus
        where bl_BMI_val is not null and fl_BMI_val is not null );

-- COMMAND ----------

-- MAGIC %md #### hypo events

-- COMMAND ----------

select count(distinct a.ptid) from ac_ehr_soliqua_hypo_lab_dx_bl a
left join ac_ehr_soliqua_demo_hba1c_bl_pts_2 b on a.ptid=b.ptid
where b.value1>=7;

select count(distinct a.DIAG_DATE) from ac_ehr_soliqua_hypo_lab_dx_bl a
left join ac_ehr_soliqua_demo_hba1c_bl_pts_2 b on a.ptid=b.ptid
where b.value1>=7;

-- select count(distinct a.ptid) from ac_ehr_soliqua_hypo_lab_dx_fu a
-- left join ac_ehr_soliqua_demo_hba1c_bl_pts_2 b on a.ptid=b.ptid
-- where b.value1>=7;

select count(distinct a.DIAG_DATE) from ac_ehr_soliqua_hypo_lab_dx_fu a
left join ac_ehr_soliqua_demo_hba1c_bl_pts_2 b on a.ptid=b.ptid
where b.value1>=7;
