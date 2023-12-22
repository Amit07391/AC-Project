-- Databricks notebook source
select distinct * from ac_ehr_sol_lab_a1c_sol_bl_fl;

select distinct * from ac_ehr_sol_lab_a1c_sol_30dys_fu_val;




-- COMMAND ----------

create or replace table ac_ehr_sol_a1c_30dys_bl_fl as
select distinct a.ptid, a.value1, b.bl_hba1c_val from ac_ehr_sol_lab_a1c_sol_30dys_fu_val a
left join ac_ehr_sol_lab_a1c_sol_bl_fl b on a.ptid=b.ptid
order by a.ptid;

select distinct * from ac_ehr_sol_a1c_30dys_bl_fl
order by ptid;

-- COMMAND ----------

select distinct * from ac_ehr_sol_a1c_30dys_bl_fl

-- COMMAND ----------

select mean(bl_hba1c_val) as mn, median(bl_hba1c_val) as med, std(bl_hba1c_val) as std, min(bl_hba1c_val) as min
        , percentile(bl_hba1c_val,.25) as p25, percentile(bl_hba1c_val,.5) as median, percentile(bl_hba1c_val,.75) as p75, max(bl_hba1c_val) as max  from ac_ehr_sol_lab_a1c_sol_bl_fl;

        select mean(fl_hba1c_val) as mn, median(fl_hba1c_val) as med, std(fl_hba1c_val) as std, min(fl_hba1c_val) as min
        , percentile(fl_hba1c_val,.25) as p25, percentile(fl_hba1c_val,.5) as median, percentile(fl_hba1c_val,.75) as p75, max(fl_hba1c_val) as max  from ac_ehr_sol_lab_a1c_sol_bl_fl;

-- COMMAND ----------

select mean(value1) as mn, median(value1) as med, std(value1) as std, min(value1) as min
        , percentile(value1,.25) as p25, percentile(value1,.5) as median, percentile(value1,.75) as p75, max(value1) as max  from ac_ehr_sol_a1c_30dys_bl_fl;  

select mean(bl_hba1c_val) as mn, median(bl_hba1c_val) as med, std(bl_hba1c_val) as std, min(bl_hba1c_val) as min
        , percentile(bl_hba1c_val,.25) as p25, percentile(bl_hba1c_val,.5) as median, percentile(bl_hba1c_val,.75) as p75, max(bl_hba1c_val) as max  from ac_ehr_sol_a1c_30dys_bl_fl;  

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_hba1c_val - fl_hba1c_val as diff from ac_ehr_sol_lab_a1c_sol_bl_fl)

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_hba1c_val - value1 as diff from ac_ehr_sol_a1c_30dys_bl_fl);

-- COMMAND ----------

select distinct ptid,bl_hba1c_val, fl_hba1c_val,  bl_hba1c_val - fl_hba1c_val as diff from ac_ehr_sol_lab_a1c_sol_bl_fl

-- COMMAND ----------

-- select count(distinct ptid) from ac_ehr_sol_lab_a1c_sol_bl_fl
-- where bl_hba1c_val<7;

-- select count(distinct ptid) from ac_ehr_sol_lab_a1c_sol_bl_fl
-- where fl_hba1c_val<7;

-- select count(distinct ptid) from ac_ehr_sol_lab_a1c_sol_bl_fl
-- where bl_hba1c_val<8;

-- select count(distinct ptid) from ac_ehr_sol_lab_a1c_sol_bl_fl
-- where fl_hba1c_val<8;

select count(distinct ptid) from ac_ehr_sol_lab_a1c_sol_30dys_fu_val
where value1<7;

select count(distinct ptid) from ac_ehr_sol_lab_a1c_sol_30dys_fu_val
where value1<8;


-- COMMAND ----------

ac_ehr_sol_WT_sol_bl_fu;

ac_ehr_sol_WT_sol_bl_val;

ac_ehr_sol_WT_sol_fu_val

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_WT_sol_bl_val; --367

select count(distinct ptid) from ac_ehr_sol_WT_sol_fu_val; --361

-- COMMAND ----------

select mean(bl_wt_val) as mn, median(bl_wt_val) as med, std(bl_wt_val) as std, min(bl_wt_val) as min
        , percentile(bl_wt_val,.25) as p25, percentile(bl_wt_val,.5) as median, percentile(bl_wt_val,.75) as p75, max(bl_wt_val) as max  from ac_ehr_sol_WT_sol_bl_fu
        where bl_wt_val is not null and fl_wt_val is not null;

        select mean(fl_wt_val) as mn, median(fl_wt_val) as med, std(fl_wt_val) as std, min(fl_wt_val) as min
        , percentile(fl_wt_val,.25) as p25, percentile(fl_wt_val,.5) as median, percentile(fl_wt_val,.75) as p75, max(fl_wt_val) as max  from ac_ehr_sol_WT_sol_bl_fu
        where bl_wt_val is not null and fl_wt_val is not null;

-- COMMAND ----------

select distinct * from ac_ehr_sol_WT_sol_bl_fu
where bl_wt_val is not null and fl_wt_val is not null 

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_wt_val - fl_wt_val as diff from ac_ehr_sol_WT_sol_bl_fu);

select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
, percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
from (select distinct ptid,  bl_wt_val - fl_wt_val as diff from ac_ehr_sol_WT_sol_bl_fu
where bl_wt_val is not null and fl_wt_val is not null )

-- COMMAND ----------

select distinct ptid,  bl_wt_val - fl_wt_val as diff from ac_ehr_sol_WT_sol_bl_fu

-- COMMAND ----------

create or replace table ac_ehr_sol_BMI_sol_bl_fu as
select distinct a.ptid, b.bl_BMI_val, c.fl_BMI_val
from ac_ehr_sol_obs_sol_tbl a
left join (select distinct ptid, value1 as bl_BMI_val from ac_ehr_sol_BMI_sol_bl_val
) b on a.ptid=b.ptid
left join (select distinct ptid, value1 as fl_BMI_val from ac_ehr_sol_BMI_sol_fu_val
) c on a.ptid=c.ptid
order by 1;

select distinct * from ac_ehr_sol_BMI_sol_bl_fu
order by 1;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_BMI_sol_bl_val; --365

select count(distinct ptid) from ac_ehr_sol_BMI_sol_fu_val; --360

-- COMMAND ----------

select mean(bl_BMI_val) as mn, median(bl_BMI_val) as med, std(bl_BMI_val) as std, min(bl_BMI_val) as min
        , percentile(bl_BMI_val,.25) as p25, percentile(bl_BMI_val,.5) as median, percentile(bl_BMI_val,.75) as p75, max(bl_BMI_val) as max  from ac_ehr_sol_BMI_sol_bl_fu
        where bl_BMI_val is not null and fl_BMI_val is not null;

        select mean(fl_BMI_val) as mn, median(fl_BMI_val) as med, std(fl_BMI_val) as std, min(fl_BMI_val) as min
        , percentile(fl_BMI_val,.25) as p25, percentile(fl_BMI_val,.5) as median, percentile(fl_BMI_val,.75) as p75, max(fl_BMI_val) as max  from ac_ehr_sol_BMI_sol_bl_fu
        where bl_BMI_val is not null and fl_BMI_val is not null;

-- COMMAND ----------

select count(distinct ptid) from ac_ehr_sol_BMI_sol_bl_fu
where bl_BMI_val is not null and fl_BMI_val is not null ;

select distinct * from ac_ehr_sol_BMI_sol_bl_fu
where bl_BMI_val is not null and fl_BMI_val is not null ;

-- COMMAND ----------

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_BMI_val - fl_BMI_val as diff from ac_ehr_sol_BMI_sol_bl_fu);

 select mean(diff) as mn, median(diff) as med, std(diff) as std, min(diff) as min
        , percentile(diff,.25) as p25, percentile(diff,.5) as median, percentile(diff,.75) as p75, max(diff) as max
        from (select distinct ptid,  bl_BMI_val - fl_BMI_val as diff from ac_ehr_sol_BMI_sol_bl_fu
        where bl_BMI_val is not null and fl_BMI_val is not null );
