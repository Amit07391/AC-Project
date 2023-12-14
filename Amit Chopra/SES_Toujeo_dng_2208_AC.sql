-- Databricks notebook source
drop table if exists ty33_pat_dx_t2dm;

create table ty33_pat_dx_t2dm as
select distinct *
from (select *, dense_rank() OVER (PARTITION BY patid ORDER BY fst_dt, diag_position, clmid, pat_planid, diag, description) as rank
      from (select * from ty19_dx_subset_17_22 where dx_name='T2DM'
            union
            select * from ty19_dx_subset_11_16 where fst_dt>='2014-07-01' and dx_name='T2DM'
            ))
order by patid
;

select dx_name, count(*) as n_obs, count(distinct patid) as n_pat, min(fst_dt) as dt_t2dm_start, max(fst_dt) as dt_t2dm_stop
from ty33_pat_dx_t2dm
group by dx_name
order by dx_name
;


-- COMMAND ----------

drop table if exists ty33_pat_dx_T1DM;

create table ty33_pat_dx_T1DM as
select distinct *
from (select *, dense_rank() OVER (PARTITION BY patid ORDER BY fst_dt, diag_position, clmid, pat_planid, diag, description) as rank
      from (select * from ty19_dx_subset_17_22 where dx_name='T1DM'
            union
            select * from ty19_dx_subset_11_16 where fst_dt>='2014-07-01' and dx_name='T1DM'
            ))
order by patid
;

select dx_name, count(*) as n_obs, count(distinct patid) as n_pat, min(fst_dt) as dt_t1dm_start, max(fst_dt) as dt_t1dm_stop
from ty33_pat_dx_T1DM
group by dx_name
order by dx_name
;


-- COMMAND ----------

drop table if exists ty33_pat_rx_basal_nph_glp1;

create table ty33_pat_rx_basal_nph_glp1 as
select distinct patid, charge,clmid,copay,days_sup,deduct,dispfee,fill_dt,quantity,specclss,std_cost,std_cost_yr,strength,brnd_nm,gnrc_nm,ndc,rx_type
       , case when lcase(brnd_nm) like '%toujeo%' then 'Toujeo'
              when lcase(gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(brnd_nm) not like '%toujeo%' then 'Gla-100'
              when lcase(gnrc_nm) like '%detemir%' then 'Detemir'
              when lcase(gnrc_nm) like '%nph%' then 'NPH'
              when lcase(rx_type) like '%glp1%' then 'GLP1'
                   else rx_type end as rx_type2
from ty19_rx_anti_dm
where fill_dt>='2014-07-01' and (lcase(rx_type) in ('basal', 'bolus', 'premix') or lcase(gnrc_nm) in ('dulaglutide','semaglutide','exenatide', 'exenatide microspheres'))
order by patid, fill_dt
;

select rx_type2, rx_type, gnrc_nm,brnd_nm, min(fill_dt) as dt_rx_start, max(fill_dt) as dt_rx_end
from ty33_pat_rx_basal_nph_glp1
group by rx_type2, rx_type, gnrc_nm,brnd_nm
order by rx_type2, rx_type, gnrc_nm,brnd_nm
;


-- COMMAND ----------

drop table if exists ty33_pat_dx_rx;

create table ty33_pat_dx_rx as
select distinct a.patid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_toujeo) as dt_1st_toujeo
        , min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) as dt_rx_other_insulin
        , min(d.dt_1st_gla_100) as dt_1st_gla_100, min(e.dt_1st_detemir) as dt_1st_detemir, min(f.dt_1st_nph) as dt_1st_nph
        , case when isnotnull(min(c.dt_1st_toujeo)) then min(c.dt_1st_toujeo)
               else min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) end as dt_rx_index
        , case when isnotnull(min(c.dt_1st_toujeo)) then 'Toujeo'
               when isnotnull(min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph))) then 'Other long-acting BIs'
               else null end as index_group
from (select distinct patid, min(fst_dt) as dt_1st_t2dm, count(distinct fst_dt) as n_t2dm from ty33_pat_dx_t2dm group by patid) a
      left join (select distinct patid, min(fst_dt) as dt_1st_t1dm, count(distinct fst_dt) as n_t1dm from ty33_pat_dx_t1dm group by patid) b on a.patid=b.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_toujeo from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Toujeo') group by patid) c on a.patid=c.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_gla_100 from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Gla-100') group by patid) d on a.patid=d.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_detemir from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Detemir') group by patid) e on a.patid=e.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_nph from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('NPH') group by patid) f on a.patid=f.patid
group by a.patid
order by a.patid
;

select count(*) as n_obs, count(distinct patid) as n_pat, min(dt_1st_t2dm) as dt_d2dm_start, max(dt_1st_t2dm) as dt_d2dm_end
from ty33_pat_dx_rx
where isnotnull(dt_rx_index)
;

select index_group, count(*) as n_obs, count(distinct patid) as n_pat, min(dt_rx_index) as dt_rx_start, max(dt_rx_index) as dt_rx_end
from ty33_pat_dx_rx
group by index_group
;


-- COMMAND ----------

drop table if exists ty33_pat_rx_basal_nph_glp1_index;

create table ty33_pat_rx_basal_nph_glp1_index as
select distinct a.*, b.dt_rx_index
from ty33_pat_rx_basal_nph_glp1 a left join ty33_pat_dx_rx b
on a.patid=b.patid
order by a.patid, a.fill_dt
;

select * from ty33_pat_rx_basal_nph_glp1_index;


-- COMMAND ----------

drop table if exists ty33_lab_a1c_index;

create table ty33_lab_a1c_index as
select a.*, b.dt_rx_index
from ty19_lab_a1c_loinc_value a join ty33_pat_dx_rx b
on a.patid=b.patid
where isnotnull(b.dt_rx_index) and a.value between 3 and 15
order by a.patid, a.fst_dt
;

select * from ty33_lab_a1c_index;


-- COMMAND ----------

drop table if exists ty33_glp1_a1c_bl_fu;

create table ty33_glp1_a1c_bl_fu as
select distinct a.patid as patid1, max(b.dt_last_glp1_bl) as dt_last_glp1_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, min(e.dt_1st_glp1_fu) as dt_1st_glp1_fu, max(f.dt_last_insulin_bl) as dt_last_insulin_bl
from (select patid, dt_rx_index from ty33_pat_dx_rx where isnotnull(dt_rx_index)) a
     left join (select distinct patid, max(fill_dt) as dt_last_glp1_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1) group by patid) b on a.patid=b.patid
     left join (select distinct patid, max(fst_dt) as dt_last_a1c_bl from ty33_lab_a1c_index where fst_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) c on a.patid=c.patid
     left join (select distinct patid, min(fst_dt) as dt_1st_a1c_fu from ty33_lab_a1c_index where fst_dt >= dt_rx_index group by patid) d on a.patid=d.patid
     left join (select distinct patid, min(fill_dt) as dt_1st_glp1_fu from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt >= dt_rx_index group by patid) e on a.patid=e.patid
     left join (select distinct patid, max(fill_dt) as dt_last_insulin_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) not in ('glp1') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) f on a.patid=f.patid
group by patid1
order by patid1
;

select * from ty33_glp1_a1c_bl_fu;

-- COMMAND ----------

drop table if exists ty33_pat_all_enrol;

create table ty33_pat_all_enrol as
select distinct a.*, b.*, c.eligeff as enrlstdt, c.eligend as enrlendt, c.gdr_cd, c.yrdob, year(a.dt_rx_index)-c.yrdob as age_index
from ty33_pat_dx_rx a left join ty33_glp1_a1c_bl_fu b on a.patid=b.patid1
                      left join ty19_ses_2208_mem_conti c on a.patid=c.patid and a.dt_rx_index between c.eligeff and c.eligend
order by a.patid
;

select * from ty33_pat_all_enrol;

-- COMMAND ----------

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol
;

-- COMMAND ----------

select distinct patid, fill_dt, rx_type, dt_rx_index 
from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1)
order by patid, fill_dt
;

-- COMMAND ----------

select distinct patid, min(fill_dt) as dt_1st_glp1_fu from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt >= dt_rx_index group by patid

-- COMMAND ----------

select distinct patid, max(fill_dt) as dt_last_insulin_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) not in ('glp1') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid

-- COMMAND ----------

drop table if exists ty33_patient_attrition;

create table ty33_patient_attrition as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 7/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm)
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 7/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition;

-- COMMAND ----------

drop table if exists ty33_patient_attrition_pct;

create table ty33_patient_attrition_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_pct
;


-- COMMAND ----------

select patid, dt_1st_t2dm, dt_1st_t1dm, dt_rx_index, dt_1st_toujeo, index_group, age_index, enrlstdt, dt_last_glp1_bl, dt_last_a1c_bl, dt_1st_a1c_fu,dt_1st_glp1_fu,dt_last_insulin_bl,dt_rx_other_insulin
from ty33_pat_all_enrol
where isnotnull(dt_1st_t2dm)
      and isnotnull(dt_rx_index)
      and age_index>=18
      and dt_rx_index>=date_add(enrlstdt,180)
      and isnotnull(dt_last_glp1_bl)
      and isnotnull(dt_last_a1c_bl)
      and isnotnull(dt_1st_a1c_fu)
      and isnotnull(dt_1st_glp1_fu)
      and isnull(dt_1st_t1dm)
      and isnull(dt_last_insulin_bl)
      and dt_1st_toujeo=dt_rx_other_insulin
order by patid
;


-- COMMAND ----------

select patid, dt_1st_t2dm, dt_1st_t1dm, dt_rx_index, dt_1st_toujeo, index_group, age_index, enrlstdt, dt_last_glp1_bl, dt_last_a1c_bl, dt_1st_a1c_fu,dt_1st_glp1_fu,dt_last_insulin_bl,dt_rx_other_insulin
from ty33_pat_all_enrol
where isnotnull(dt_1st_t2dm)
      and isnotnull(dt_rx_index)
--    and age_index>=18
--    and dt_rx_index>=date_add(enrlstdt,180)
--    and isnotnull(dt_last_glp1_bl)
--    and isnotnull(dt_last_a1c_bl)
--    and isnotnull(dt_1st_a1c_fu)
--    and isnotnull(dt_1st_glp1_fu)
--    and isnull(dt_1st_t1dm)
--    and isnull(dt_last_insulin_bl)
      and dt_1st_toujeo=dt_rx_other_insulin
order by patid
;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_pct")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

select * from ty19_ses_2208_mem_enrol;

-- COMMAND ----------

drop table if exists ty33_pat_all_enrol_demog;

create table ty33_pat_all_enrol_demog as
select distinct a.*, b.eligeff, b.eligend, b.bus, b.division,b.product
from (select *, 1 as fl_study_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))) a left join ty19_ses_2208_mem_enrol b
on a.patid=b.patid and a.dt_rx_index between b.eligeff and b.eligend
order by a.patid
;

select * from ty33_pat_all_enrol_demog;

-- COMMAND ----------

select * from ty33_pat_all_enrol_demog;

-- COMMAND ----------

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol_demog
;

-- COMMAND ----------

drop table if exists ty33_fu_6m_a1c;

create table ty33_fu_6m_a1c as
select distinct patid, fst_dt as dt_last_a1c_fu_6m, value as bl_6m_a1c_last, rank
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid
;

select * from ty33_fu_6m_a1c;

-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_fu_6m) as dt_a1c_start, max(dt_last_a1c_fu_6m) as dt_a1c_stop
from ty33_fu_6m_a1c
;

-- COMMAND ----------

drop table if exists ty33_bl_6m_a1c;

create table ty33_bl_6m_a1c as
select distinct patid, fst_dt as dt_last_a1c_bl_6m, value as fu_6m_a1c_last, rank
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid
;

select * from ty33_bl_6m_a1c;


-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_bl_6m) as dt_a1c_start, max(dt_last_a1c_bl_6m) as dt_a1c_stop
from ty33_bl_6m_a1c
;

-- COMMAND ----------

drop table if exists ty33_fu_6m_med;

create table ty33_fu_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med;

-- COMMAND ----------

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_fu_6m_med

;

-- COMMAND ----------

drop table if exists ty33_fu_6m_med;

create table ty33_fu_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_fu_6m_med
;


-- COMMAND ----------

drop table if exists ty33_fu_6m_med_dm;

create table ty33_fu_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_fu_6m_med a join (select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                            union
                            select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                            ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med_dm;

-- COMMAND ----------

drop table if exists ty33_fu_6m_med_dm_max;

create table ty33_fu_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_fu_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_fu_6m_med_dm_max;


-- COMMAND ----------

select count(*) from ty33_fu_6m_med_dm_max;

-- COMMAND ----------

select count(*) from ty33_fu_6m_med_dm_max;

-- COMMAND ----------

drop table if exists ty33_fu_6m_rx;

create table ty33_fu_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx;


-- COMMAND ----------

drop table if exists ty33_fu_6m_rx_anti_dm;

create table ty33_fu_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_fu_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx_anti_dm;

-- COMMAND ----------

drop table if exists ty33_pat_fu_6m_cost;

create table ty33_pat_fu_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.fu_6m_cost_med) as fu_6m_cost_med, max(b.fu_6m_cost_med_inp) as fu_6m_cost_med_inp, max(c.fu_6m_cost_med_er) as fu_6m_cost_med_er, max(d.fu_6m_cost_med_dm) as fu_6m_cost_med_dm
        , max(e.fu_6m_cost_med_dm_inp) as fu_6m_cost_med_dm_inp, max(f.fu_6m_cost_med_dm_er) as fu_6m_cost_med_dm_er, max(g.fu_6m_cost_rx) as fu_6m_cost_rx, max(h.fu_6m_cost_rx_anti_dm) as fu_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as fu_6m_cost_med from ty33_fu_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_inp from ty33_fu_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_er from ty33_fu_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm from ty33_fu_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_inp from ty33_fu_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_er from ty33_fu_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx from ty33_fu_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx_anti_dm from ty33_fu_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_cost
;


-- COMMAND ----------

drop table if exists ty33_bl_6m_med;

create table ty33_bl_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med;

drop table if exists ty33_bl_6m_med_dm;

create table ty33_bl_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_bl_6m_med a join (select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med_dm;

drop table if exists ty33_bl_6m_med_dm_max;

create table ty33_bl_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_bl_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_bl_6m_med_dm_max;

drop table if exists ty33_bl_6m_rx;

create table ty33_bl_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx;

drop table if exists ty33_bl_6m_rx_anti_dm;

create table ty33_bl_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_bl_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx_anti_dm;

drop table if exists ty33_pat_bl_6m_cost;

create table ty33_pat_bl_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.bl_6m_cost_med) as bl_6m_cost_med, max(b.bl_6m_cost_med_inp) as bl_6m_cost_med_inp, max(c.bl_6m_cost_med_er) as bl_6m_cost_med_er, max(d.bl_6m_cost_med_dm) as bl_6m_cost_med_dm
        , max(e.bl_6m_cost_med_dm_inp) as bl_6m_cost_med_dm_inp, max(f.bl_6m_cost_med_dm_er) as bl_6m_cost_med_dm_er, max(g.bl_6m_cost_rx) as bl_6m_cost_rx, max(h.bl_6m_cost_rx_anti_dm) as bl_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as bl_6m_cost_med from ty33_bl_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_inp from ty33_bl_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_er from ty33_bl_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm from ty33_bl_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_inp from ty33_bl_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_er from ty33_bl_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx from ty33_bl_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx_anti_dm from ty33_bl_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_cost
;


-- COMMAND ----------

drop table if exists ty33_patient_attrition_toujeo;

create table ty33_patient_attrition_toujeo as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 7/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm)
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 7/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_toujeo;


-- COMMAND ----------

drop table if exists ty33_patient_attrition_toujeo_pct;

create table ty33_patient_attrition_toujeo_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_toujeo)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_toujeo_pct
;


-- COMMAND ----------

drop table if exists ty33_patient_attrition_other;

create table ty33_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 7/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where isnotnull(dt_1st_t2dm)
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 7/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index)
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and isnotnull(dt_1st_t2dm) and isnotnull(dt_rx_index) and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_other;

drop table if exists ty33_patient_attrition_other_pct;

create table ty33_patient_attrition_other_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_other)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_other_pct
;



-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_toujeo_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_toujeo_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_other_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_other_pct")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_comorb1;

create table ty33_fu_6m_dx_comorb1 as
select distinct patid, dx_name, fst_dt as dt_last_dx_fu_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_11_16 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, dx_name
;

select * from ty33_fu_6m_dx_comorb1;

-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_comorb2;

create table ty33_fu_6m_dx_comorb2 as
select distinct patid, dx_name, fst_dt as dt_last_dx_fu_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_17_22 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, dx_name
;

select * from ty33_fu_6m_dx_comorb2;

-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_comorb;

create table ty33_fu_6m_dx_comorb as
select distinct coalesce(a.patid, b.patid) as patid, coalesce(a.dx_name, b.dx_name) as dx_name, coalesce(a.Disease, b.Disease) as Disease, coalesce(a.weight, b.weight) as weight
        , coalesce(a.weight_old, b.weight_old) as weight_old, greatest(a.dt_last_dx_fu_6m, b.dt_last_dx_fu_6m) as dt_last_dx_fu_6m
from ty33_fu_6m_dx_comorb1 a full join ty33_fu_6m_dx_comorb2 b
on a.patid=b.patid and a.dx_name=b.dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_fu_6m) as dt_dx_start, max(dt_last_dx_fu_6m) as dt_dx_stop
from ty33_fu_6m_dx_comorb
group by dx_name
order by dx_name
;


-- COMMAND ----------

drop table if exists ty33_bl_6m_dx_comorb1;

create table ty33_bl_6m_dx_comorb1 as
select distinct patid, dx_name, fst_dt as dt_last_dx_bl_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_11_16 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, dx_name
;

select * from ty33_bl_6m_dx_comorb1;

drop table if exists ty33_bl_6m_dx_comorb2;

create table ty33_bl_6m_dx_comorb2 as
select distinct patid, dx_name, fst_dt as dt_last_dx_bl_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_17_22 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, dx_name
;

drop table if exists ty33_bl_6m_dx_comorb;

create table ty33_bl_6m_dx_comorb as
select distinct coalesce(a.patid, b.patid) as patid, coalesce(a.dx_name, b.dx_name) as dx_name, coalesce(a.Disease, b.Disease) as Disease, coalesce(a.weight, b.weight) as weight
        , coalesce(a.weight_old, b.weight_old) as weight_old, greatest(a.dt_last_dx_bl_6m, b.dt_last_dx_bl_6m) as dt_last_dx_bl_6m
from ty33_bl_6m_dx_comorb1 a full join ty33_bl_6m_dx_comorb2 b
on a.patid=b.patid and a.dx_name=b.dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_bl_6m) as dt_dx_start, max(dt_last_dx_bl_6m) as dt_dx_stop
from ty33_bl_6m_dx_comorb
group by dx_name
order by dx_name
;


-- COMMAND ----------

drop table if exists ty33_fu_6m_inp_er;

create table ty33_fu_6m_inp_er as
select distinct a.patid, a.clmid, a.fst_dt, a.lst_dt, a.pos, a.proc_cd, a.conf_id, b.dt_rx_index
from (select * from ty19_ses_2208_med_claim where pos in ('21', '23')) a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_inp_er;

-- COMMAND ----------

drop table if exists ty33_fu_6m_inp_er_dm;

create table ty33_fu_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_fu_6m_inp_er a join (select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_inp_er_dm;


-- COMMAND ----------

drop table if exists ty33_pat_fu_6m_inp_er;

create table ty33_pat_fu_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.fu_6m_inp) as fu_6m_inp, max(e.fu_6m_inp_n) as fu_6m_inp_n, max(e.fu_6m_inp_days) as fu_6m_inp_days, max(b.fu_6m_er) as fu_6m_er, max(f.fu_6m_er_n) as fu_6m_er_n
        , max(c.fu_6m_inp_dm) as fu_6m_inp_dm, max(g.fu_6m_inp_n_dm) as fu_6m_inp_n_dm, max(g.fu_6m_inp_days_dm) as fu_6m_inp_days_dm
        , max(d.fu_6m_er_dm) as fu_6m_er_dm, max(h.fu_6m_er_n_dm) as fu_6m_er_n_dm
from (select distinct patid, 1 as fu_6m_inp from ty33_fu_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as fu_6m_er from ty33_fu_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as fu_6m_inp_dm from ty33_fu_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as fu_6m_er_dm from ty33_fu_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days from ty33_fu_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n from ty33_fu_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days_dm from ty33_fu_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n_dm from ty33_fu_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_inp_er
;


-- COMMAND ----------

select a.*, b.n_obs
from ty33_pat_fu_6m_inp_er a join (select patid1, count(*) as n_obs from ty33_pat_fu_6m_inp_er group by patid1) b
on a.patid1=b.patid1
where b.n_obs>1
order by a.patid1
;


-- COMMAND ----------

drop table if exists ty33_bl_6m_inp_er;

create table ty33_bl_6m_inp_er as
select distinct a.patid, a.clmid, a.fst_dt, a.lst_dt, a.pos, a.proc_cd, a.conf_id, b.dt_rx_index
from (select * from ty19_ses_2208_med_claim where pos in ('21', '23')) a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er;

drop table if exists ty33_bl_6m_inp_er_dm;

create table ty33_bl_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_bl_6m_inp_er a join (select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er_dm;

drop table if exists ty33_pat_bl_6m_inp_er;

create table ty33_pat_bl_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.bl_6m_inp) as bl_6m_inp, max(e.bl_6m_inp_n) as bl_6m_inp_n, max(e.bl_6m_inp_days) as bl_6m_inp_days, max(b.bl_6m_er) as bl_6m_er, max(f.bl_6m_er_n) as bl_6m_er_n
        , max(c.bl_6m_inp_dm) as bl_6m_inp_dm, max(g.bl_6m_inp_n_dm) as bl_6m_inp_n_dm, max(g.bl_6m_inp_days_dm) as bl_6m_inp_days_dm
        , max(d.bl_6m_er_dm) as bl_6m_er_dm, max(h.bl_6m_er_n_dm) as bl_6m_er_n_dm
from (select distinct patid, 1 as bl_6m_inp from ty33_bl_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as bl_6m_er from ty33_bl_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as bl_6m_inp_dm from ty33_bl_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as bl_6m_er_dm from ty33_bl_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days from ty33_bl_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n from ty33_bl_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days_dm from ty33_bl_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n_dm from ty33_bl_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_inp_er
;


-- COMMAND ----------

drop table if exists ty33_fu_6m_rx;

create table ty33_fu_6m_rx as
select distinct patid, rx_type, fill_dt as dt_last_rx_fu_6m, dt_rx_index, gnrc_nm, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.rx_type order by a.fill_dt desc, a.ndc) as rank from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fill_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, rx_type
;

select * from ty33_fu_6m_rx;

select rx_type, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_rx_fu_6m) as dt_rx_start, max(dt_last_rx_fu_6m) as dt_rx_stop
from ty33_fu_6m_rx
group by rx_type
order by rx_type
;


-- COMMAND ----------

drop table if exists ty33_bl_6m_rx;

create table ty33_bl_6m_rx as
select distinct patid, rx_type, fill_dt as dt_last_rx_bl_6m, dt_rx_index, gnrc_nm, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.rx_type order by a.fill_dt desc, a.ndc) as rank from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, rx_type
;

select * from ty33_bl_6m_rx;

select rx_type, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_rx_bl_6m) as dt_rx_start, max(dt_last_rx_bl_6m) as dt_rx_stop
from ty33_bl_6m_rx
group by rx_type
order by rx_type
;


-- COMMAND ----------

drop table if exists ty33_index_social;

create table ty33_index_social as
select distinct a.patid, d_education_level_code,d_fed_poverty_status_code,d_home_ownership_code,d_household_income_range_code,d_networth_range_code,d_occupation_type_code,d_race_code,num_adults,num_child, dt_rx_index
from ty19_ses_2208_Socioeconomic a join ty33_pat_all_enrol_demog b
on a.patid=b.patid
order by a.patid
;

select * from ty33_index_social;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_rx_index) as dt_start, max(dt_rx_index) as dt_stop
from ty33_index_social
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_a1c")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_a1c")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_dx_comorb")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_dx_comorb")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_dx_comorb")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_dx_comorb")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_inp_er")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_inp_er")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_inp_er")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_inp_er")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_rx")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_rx")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_rx")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_rx")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_index_social")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_index_social")
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_pat_all_enrol_demog")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_all_enrol_demog")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

select *
from ty00_all_dx_comorb
where dx_name='HYPO'
--group by dx_name, disease
order by dx_name
;

-- COMMAND ----------

drop table if exists ty33_fu_6m_a1c;

create table ty33_fu_6m_a1c as
select distinct patid, fst_dt as dt_last_a1c_fu_6m, value as fu_6m_a1c_last, rank
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210))
where rank<=1
order by patid
;

select * from ty33_fu_6m_a1c;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

select tst_desc, count(*) as n_obs 
from ty19_ses_2208_lab_result
where ucase(tst_desc) like '%ESTIMATED AVERAGE GLUCOSE%'
group by tst_desc
order by tst_desc
;


-- COMMAND ----------

select *
from ty19_ses_2208_lab_result
where ucase(tst_desc) like '%ESTIMATED AVERAGE GLUCOSE%'
--group by tst_desc
order by tst_desc
;


-- COMMAND ----------

select *
from ty19_ses_2208_lookup_ndc
where lcase(BRND_NM) like '%toujeo%'
;

-- COMMAND ----------

select dx_name, disease, count(*) as n_code
from ty00_all_dx_comorb
group by dx_name, disease
order by dx_name
;

-- COMMAND ----------

drop table if exists ty33_pat_dx_T1DM;

create table ty33_pat_dx_T1DM as
select distinct *
from (select *, dense_rank() OVER (PARTITION BY patid ORDER BY fst_dt, diag_position, clmid, pat_planid, diag, description) as rank
      from (select * from ty19_dx_subset_17_22 where dx_name in ('T1DM', 'DM_2nd')
            union
            select * from ty19_dx_subset_11_16 where fst_dt>='2014-07-01' and dx_name in ('T1DM', 'DM_2nd')
            ))
order by patid
;

select dx_name, count(*) as n_obs, count(distinct patid) as n_pat, min(fst_dt) as dt_t1dm_start, max(fst_dt) as dt_t1dm_stop
from ty33_pat_dx_T1DM
group by dx_name
order by dx_name
;


-- COMMAND ----------

select *
from ty19_rx_anti_dm
where fill_dt>='2014-07-01' and lcase(gnrc_nm) in ('dulaglutide','semaglutide','exenatide', 'exenatide microspheres')
order by patid, fill_dt
;


-- COMMAND ----------

drop table if exists ty33_pat_rx_basal_nph_glp1;

create table ty33_pat_rx_basal_nph_glp1 as
select distinct patid, charge,clmid,copay,days_sup,deduct,dispfee,fill_dt,quantity,specclss,std_cost,std_cost_yr,strength,brnd_nm,gnrc_nm,ndc,rx_type
       , case when lcase(brnd_nm) like '%toujeo solostar%' then 'Toujeo'
              when lcase(gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(brnd_nm) not like '%toujeo%' then 'Gla-100'
              when lcase(gnrc_nm) like '%detemir%' then 'Detemir'
              when lcase(gnrc_nm) like '%nph%' then 'NPH'
              when lcase(rx_type) like '%glp1%' then 'GLP1'
                   else rx_type end as rx_type2
from ty19_rx_anti_dm
where fill_dt>='2014-07-01' and (lcase(rx_type) in ('basal', 'bolus', 'premix')
        or (lcase(gnrc_nm) in ('dulaglutide','semaglutide','exenatide', 'exenatide microspheres') and DOSAGE_FM_DESC!='TABLET'))
order by patid, fill_dt
;

select rx_type2, rx_type, gnrc_nm,brnd_nm, min(fill_dt) as dt_rx_start, max(fill_dt) as dt_rx_end
from ty33_pat_rx_basal_nph_glp1
group by rx_type2, rx_type, gnrc_nm,brnd_nm
order by rx_type2, rx_type, gnrc_nm,brnd_nm
;


-- COMMAND ----------

drop table if exists ty33_pat_dx_rx;

create table ty33_pat_dx_rx as
select distinct a.patid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_toujeo) as dt_1st_toujeo
        , min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) as dt_rx_other_insulin
        , min(d.dt_1st_gla_100) as dt_1st_gla_100, min(e.dt_1st_detemir) as dt_1st_detemir, min(f.dt_1st_nph) as dt_1st_nph
        , case when isnotnull(min(c.dt_1st_toujeo)) then min(c.dt_1st_toujeo)
               else min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) end as dt_rx_index
        , case when isnotnull(min(c.dt_1st_toujeo)) then 'Toujeo'
               when isnotnull(min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph))) then 'Other long-acting BIs'
               else null end as index_group
from (select distinct patid, min(fst_dt) as dt_1st_t2dm, count(distinct fst_dt) as n_t2dm from ty33_pat_dx_t2dm group by patid) a
      left join (select distinct patid, min(fst_dt) as dt_1st_t1dm, count(distinct fst_dt) as n_t1dm from ty33_pat_dx_t1dm group by patid) b on a.patid=b.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_toujeo from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Toujeo') group by patid) c on a.patid=c.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_gla_100 from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Gla-100') group by patid) d on a.patid=d.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_detemir from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Detemir') group by patid) e on a.patid=e.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_nph from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('NPH') group by patid) f on a.patid=f.patid
group by a.patid
order by a.patid
;

select count(*) as n_obs, count(distinct patid) as n_pat, min(dt_1st_t2dm) as dt_d2dm_start, max(dt_1st_t2dm) as dt_d2dm_end
from ty33_pat_dx_rx
where isnotnull(dt_rx_index)
;

select index_group, count(*) as n_obs, count(distinct patid) as n_pat, min(dt_rx_index) as dt_rx_start, max(dt_rx_index) as dt_rx_end
from ty33_pat_dx_rx
group by index_group
;

drop table if exists ty33_pat_rx_basal_nph_glp1_index;

create table ty33_pat_rx_basal_nph_glp1_index as
select distinct a.*, b.dt_rx_index
from ty33_pat_rx_basal_nph_glp1 a left join ty33_pat_dx_rx b
on a.patid=b.patid
order by a.patid, a.fill_dt
;

select * from ty33_pat_rx_basal_nph_glp1_index;



-- COMMAND ----------

drop table if exists ty33_lab_a1c_index;

create table ty33_lab_a1c_index as
select a.*, b.dt_rx_index
from ty19_lab_a1c_loinc_value a join ty33_pat_dx_rx b
on a.patid=b.patid
where isnotnull(b.dt_rx_index) and a.value between 3 and 15
order by a.patid, a.fst_dt
;

select * from ty33_lab_a1c_index;

drop table if exists ty33_glp1_a1c_bl_fu;

create table ty33_glp1_a1c_bl_fu as
select distinct a.patid as patid1, max(b.dt_last_glp1_bl) as dt_last_glp1_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, min(e.dt_1st_glp1_fu) as dt_1st_glp1_fu, max(f.dt_last_insulin_bl) as dt_last_insulin_bl
from (select patid, dt_rx_index from ty33_pat_dx_rx where isnotnull(dt_rx_index)) a
     left join (select distinct patid, max(fill_dt) as dt_last_glp1_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1) group by patid) b on a.patid=b.patid
     left join (select distinct patid, max(fst_dt) as dt_last_a1c_bl from ty33_lab_a1c_index where fst_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) c on a.patid=c.patid
     left join (select distinct patid, min(fst_dt) as dt_1st_a1c_fu from ty33_lab_a1c_index where fst_dt between date_add(dt_rx_index,90) and date_add(dt_rx_index,210) group by patid) d on a.patid=d.patid
     left join (select distinct patid, min(fill_dt) as dt_1st_glp1_fu from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt >= dt_rx_index group by patid) e on a.patid=e.patid
     left join (select distinct patid, max(fill_dt) as dt_last_insulin_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) not in ('glp1') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) f on a.patid=f.patid
group by patid1
order by patid1
;

select * from ty33_glp1_a1c_bl_fu;

drop table if exists ty33_pat_all_enrol;

create table ty33_pat_all_enrol as
select distinct a.*, b.*, c.eligeff as enrlstdt, c.eligend as enrlendt, c.gdr_cd, c.yrdob, year(a.dt_rx_index)-c.yrdob as age_index
from ty33_pat_dx_rx a left join ty33_glp1_a1c_bl_fu b on a.patid=b.patid1
                      left join ty19_ses_2208_mem_conti c on a.patid=c.patid and a.dt_rx_index between c.eligeff and c.eligend
order by a.patid
;

select * from ty33_pat_all_enrol;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol
;


-- COMMAND ----------

select * from ty33_pat_all_enrol
where isnotnull(index_group)
;

-- COMMAND ----------

drop table if exists ty33_pat_all_enrol;

create table ty33_pat_all_enrol as
select distinct a.*, case when index_group='Toujeo' then index_group
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_gla_100 then 'Gla-100'
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_detemir then 'Detemir'
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_nph then 'NPH'
                          else null end as index_group2
                   , case when index_group='Toujeo' then dt_rx_index
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_gla_100 then dt_1st_gla_100
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_detemir then dt_1st_detemir
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_nph then dt_1st_nph
                          else null end as dt_rx_index2
                   , b.*, c.eligeff as enrlstdt, c.eligend as enrlendt, c.gdr_cd, c.yrdob, year(a.dt_rx_index)-c.yrdob as age_index
from ty33_pat_dx_rx a left join ty33_glp1_a1c_bl_fu b on a.patid=b.patid1
                      left join ty19_ses_2208_mem_conti c on a.patid=c.patid and a.dt_rx_index between c.eligeff and c.eligend
order by a.patid
;

select * from ty33_pat_all_enrol;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol
;


-- COMMAND ----------

select patid,dt_1st_toujeo,dt_rx_other_insulin,dt_1st_gla_100,dt_1st_detemir,dt_1st_nph,dt_rx_index,index_group, dt_rx_index2,index_group2  
from ty33_pat_all_enrol
where isnotnull(dt_rx_index)
;

-- COMMAND ----------

drop table if exists ty33_patient_attrition;

create table ty33_patient_attrition as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition;

drop table if exists ty33_patient_attrition_pct;

create table ty33_patient_attrition_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_pct
;

drop table if exists ty33_patient_attrition_toujeo;

create table ty33_patient_attrition_toujeo as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_toujeo;

drop table if exists ty33_patient_attrition_toujeo_pct;

create table ty33_patient_attrition_toujeo_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_toujeo)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_toujeo_pct
;

drop table if exists ty33_patient_attrition_other;

create table ty33_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_other;

drop table if exists ty33_patient_attrition_other_pct;

create table ty33_patient_attrition_other_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_other)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_other_pct
;

drop table if exists ty33_pat_all_enrol_demog;

create table ty33_pat_all_enrol_demog as
select distinct a.*, b.eligeff, b.eligend, b.bus, b.division,b.product
from (select *, 1 as fl_study_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2022-06-30' and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))) a left join ty19_ses_2208_mem_enrol b
on a.patid=b.patid and a.dt_rx_index between b.eligeff and b.eligend
order by a.patid
;

select * from ty33_pat_all_enrol_demog;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol_demog
;


-- COMMAND ----------

drop table if exists ty33_patient_attrition;

create table ty33_patient_attrition as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition;

drop table if exists ty33_patient_attrition_pct;

create table ty33_patient_attrition_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_pct
;

drop table if exists ty33_patient_attrition_toujeo;

create table ty33_patient_attrition_toujeo as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_toujeo;

drop table if exists ty33_patient_attrition_toujeo_pct;

create table ty33_patient_attrition_toujeo_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_toujeo)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_toujeo_pct
;

drop table if exists ty33_patient_attrition_other;

create table ty33_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_other;

drop table if exists ty33_patient_attrition_other_pct;

create table ty33_patient_attrition_other_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_other)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_other_pct
;

drop table if exists ty33_patient_attrition_gla100;

create table ty33_patient_attrition_gla100 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_gla_100))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_gla_100)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_gla100;

drop table if exists ty33_patient_attrition_gla100_pct;

create table ty33_patient_attrition_gla100_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_gla100)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_gla100_pct
;

drop table if exists ty33_patient_attrition_detemir;

create table ty33_patient_attrition_detemir as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_detemir))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_detemir)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_detemir;

drop table if exists ty33_patient_attrition_detemir_pct;

create table ty33_patient_attrition_detemir_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_detemir)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_detemir_pct
;

drop table if exists ty33_patient_attrition_nph;

create table ty33_patient_attrition_nph as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_nph))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_nph)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_nph;

drop table if exists ty33_patient_attrition_nph_pct;

create table ty33_patient_attrition_nph_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_nph)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_nph_pct
;

drop table if exists ty33_pat_all_enrol_demog;

create table ty33_pat_all_enrol_demog as
select distinct a.*, b.eligeff, b.eligend, b.bus, b.division,b.product
from (select *, 1 as fl_study_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))) a left join ty19_ses_2208_mem_enrol b
on a.patid=b.patid and a.dt_rx_index between b.eligeff and b.eligend
order by a.patid
;

select * from ty33_pat_all_enrol_demog;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol_demog
;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_toujeo_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_toujeo_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_other_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_other_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_gla100_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_gla100_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_detemir_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_detemir_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_nph_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_nph_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

drop table if exists ty33_patient_attrition_gla100;

create table ty33_patient_attrition_gla100 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_gla100;

drop table if exists ty33_patient_attrition_gla100_pct;

create table ty33_patient_attrition_gla100_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_gla100)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_gla100_pct
;


-- COMMAND ----------

drop table if exists ty33_patient_attrition_detemir;

create table ty33_patient_attrition_detemir as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_detemir;

drop table if exists ty33_patient_attrition_detemir_pct;

create table ty33_patient_attrition_detemir_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_detemir)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_detemir_pct
;


-- COMMAND ----------

drop table if exists ty33_patient_attrition_nph;

create table ty33_patient_attrition_nph as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_nph;

drop table if exists ty33_patient_attrition_nph_pct;

create table ty33_patient_attrition_nph_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_nph)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_nph_pct
;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_gla100_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_gla100_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_detemir_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_detemir_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_nph_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_nph_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

select * from ty33_pat_all_enrol;

-- COMMAND ----------

drop table if exists ty33_pat_dx_t2dm;

create table ty33_pat_dx_t2dm as
select distinct *
from (select *, dense_rank() OVER (PARTITION BY patid ORDER BY fst_dt, diag_position, clmid, pat_planid, diag, description) as rank
      from (select * from ty19_dx_subset_17_22 where dx_name='T2DM'
            union
            select * from ty19_dx_subset_11_16 where fst_dt>='2014-07-01' and dx_name='T2DM'
            ))
order by patid
;

select dx_name, count(*) as n_obs, count(distinct patid) as n_pat, min(fst_dt) as dt_t2dm_start, max(fst_dt) as dt_t2dm_stop
from ty33_pat_dx_t2dm
group by dx_name
order by dx_name
;

select a.*, b.n_obs
from ty33_pat_dx_t2dm a join (select patid, count(*) as n_obs from ty33_pat_dx_t2dm group by patid) b
on a.patid=b.patid
where b.n_obs>1
order by patid
;

drop table if exists ty33_pat_dx_T1DM;

create table ty33_pat_dx_T1DM as
select distinct *
from (select *, dense_rank() OVER (PARTITION BY patid ORDER BY fst_dt, diag_position, clmid, pat_planid, diag, description) as rank
      from (select * from ty19_dx_subset_17_22 where dx_name in ('T1DM', 'DM_2nd')
            union
            select * from ty19_dx_subset_11_16 where fst_dt>='2014-07-01' and dx_name in ('T1DM', 'DM_2nd')
            ))
order by patid
;

select dx_name, count(*) as n_obs, count(distinct patid) as n_pat, min(fst_dt) as dt_t1dm_start, max(fst_dt) as dt_t1dm_stop
from ty33_pat_dx_T1DM
group by dx_name
order by dx_name
;

drop table if exists ty33_pat_rx_basal_nph_glp1;

create table ty33_pat_rx_basal_nph_glp1 as
select distinct patid, charge,clmid,copay,days_sup,deduct,dispfee,fill_dt,quantity,specclss,std_cost,std_cost_yr,strength,brnd_nm,gnrc_nm,ndc,rx_type
       , case when lcase(brnd_nm) like '%toujeo solostar%' then 'Toujeo'
              when lcase(gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(brnd_nm) not like '%toujeo%' then 'Gla-100'
              when lcase(gnrc_nm) like '%detemir%' then 'Detemir'
              when lcase(gnrc_nm) like '%nph%' then 'NPH'
              when lcase(rx_type) like '%glp1%' then 'GLP1'
                   else rx_type end as rx_type2
from ty19_rx_anti_dm
where fill_dt>='2014-07-01' and (lcase(rx_type) in ('basal', 'bolus', 'premix')
        or (lcase(gnrc_nm) in ('dulaglutide','semaglutide','exenatide', 'exenatide microspheres') and DOSAGE_FM_DESC!='TABLET'))
order by patid, fill_dt
;

select rx_type2, rx_type, gnrc_nm,brnd_nm, min(fill_dt) as dt_rx_start, max(fill_dt) as dt_rx_end
from ty33_pat_rx_basal_nph_glp1
group by rx_type2, rx_type, gnrc_nm,brnd_nm
order by rx_type2, rx_type, gnrc_nm,brnd_nm
;

drop table if exists ty33_pat_dx_rx;

create table ty33_pat_dx_rx as
select distinct a.patid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_toujeo) as dt_1st_toujeo
        , min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) as dt_rx_other_insulin
        , min(d.dt_1st_gla_100) as dt_1st_gla_100, min(e.dt_1st_detemir) as dt_1st_detemir, min(f.dt_1st_nph) as dt_1st_nph
        , case when isnotnull(min(c.dt_1st_toujeo)) then min(c.dt_1st_toujeo)
               else min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) end as dt_rx_index
        , case when isnotnull(min(c.dt_1st_toujeo)) then 'Toujeo'
               when isnotnull(min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph))) then 'Other long-acting BIs'
               else null end as index_group
from (select distinct patid, min(fst_dt) as dt_1st_t2dm, count(distinct fst_dt) as n_t2dm from ty33_pat_dx_t2dm group by patid) a
      left join (select distinct patid, min(fst_dt) as dt_1st_t1dm, count(distinct fst_dt) as n_t1dm from ty33_pat_dx_t1dm group by patid) b on a.patid=b.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_toujeo from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Toujeo') group by patid) c on a.patid=c.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_gla_100 from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Gla-100') group by patid) d on a.patid=d.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_detemir from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Detemir') group by patid) e on a.patid=e.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_nph from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('NPH') group by patid) f on a.patid=f.patid
group by a.patid
order by a.patid
;

select count(*) as n_obs, count(distinct patid) as n_pat, min(dt_1st_t2dm) as dt_d2dm_start, max(dt_1st_t2dm) as dt_d2dm_end
from ty33_pat_dx_rx
where isnotnull(dt_rx_index)
;

select index_group, count(*) as n_obs, count(distinct patid) as n_pat, min(dt_rx_index) as dt_rx_start, max(dt_rx_index) as dt_rx_end
from ty33_pat_dx_rx
group by index_group
;

drop table if exists ty33_pat_rx_basal_nph_glp1_index;

create table ty33_pat_rx_basal_nph_glp1_index as
select distinct a.*, b.dt_rx_index
from ty33_pat_rx_basal_nph_glp1 a left join ty33_pat_dx_rx b
on a.patid=b.patid
order by a.patid, a.fill_dt
;

select * from ty33_pat_rx_basal_nph_glp1_index;

drop table if exists ty33_lab_a1c_index;

create table ty33_lab_a1c_index as
select a.*, b.dt_rx_index
from ty19_lab_a1c_loinc_value a join ty33_pat_dx_rx b
on a.patid=b.patid
where isnotnull(b.dt_rx_index) and a.value between 3 and 15
order by a.patid, a.fst_dt
;

select * from ty33_lab_a1c_index;

drop table if exists ty33_glp1_a1c_bl_fu;

create table ty33_glp1_a1c_bl_fu as
select distinct a.patid as patid1, max(b.dt_last_glp1_bl) as dt_last_glp1_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, min(e.dt_1st_glp1_fu) as dt_1st_glp1_fu, max(f.dt_last_insulin_bl) as dt_last_insulin_bl
from (select patid, dt_rx_index from ty33_pat_dx_rx where isnotnull(dt_rx_index)) a
     left join (select distinct patid, max(fill_dt) as dt_last_glp1_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1) group by patid) b on a.patid=b.patid
     left join (select distinct patid, max(fst_dt) as dt_last_a1c_bl from ty33_lab_a1c_index where fst_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) c on a.patid=c.patid
     left join (select distinct patid, min(fst_dt) as dt_1st_a1c_fu from ty33_lab_a1c_index where fst_dt between date_add(dt_rx_index,90) and date_add(dt_rx_index,210) group by patid) d on a.patid=d.patid
     left join (select distinct patid, min(fill_dt) as dt_1st_glp1_fu from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt >= dt_rx_index group by patid) e on a.patid=e.patid
     left join (select distinct patid, max(fill_dt) as dt_last_insulin_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) not in ('glp1') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) f on a.patid=f.patid
group by patid1
order by patid1
;

select * from ty33_glp1_a1c_bl_fu;

drop table if exists ty33_pat_all_enrol;

create table ty33_pat_all_enrol as
select distinct a.*, case when index_group='Toujeo' then index_group
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_gla_100 then 'Gla-100'
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_detemir then 'Detemir'
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_nph then 'NPH'
                          else null end as index_group2
                   , case when index_group='Toujeo' then dt_rx_index
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_gla_100 then dt_1st_gla_100
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_detemir then dt_1st_detemir
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_nph then dt_1st_nph
                          else null end as dt_rx_index2
                   , b.*, c.eligeff as enrlstdt, c.eligend as enrlendt, c.gdr_cd, c.yrdob, year(a.dt_rx_index)-c.yrdob as age_index
from ty33_pat_dx_rx a left join ty33_glp1_a1c_bl_fu b on a.patid=b.patid1
                      left join ty19_ses_2208_mem_conti c on a.patid=c.patid and a.dt_rx_index between c.eligeff and c.eligend
order by a.patid
;

select * from ty33_pat_all_enrol;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol
;

select patid,dt_1st_toujeo,dt_rx_other_insulin,dt_1st_gla_100,dt_1st_detemir,dt_1st_nph,dt_rx_index,index_group, dt_rx_index2,index_group2
from ty33_pat_all_enrol
where isnotnull(dt_rx_index)
;

drop table if exists ty33_patient_attrition;

create table ty33_patient_attrition as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition;

drop table if exists ty33_patient_attrition_pct;

create table ty33_patient_attrition_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_pct
;

drop table if exists ty33_patient_attrition_toujeo;

create table ty33_patient_attrition_toujeo as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) and n_t2dm>n_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) and n_t2dm>n_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) and n_t2dm>n_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) and n_t2dm>n_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_toujeo;

drop table if exists ty33_patient_attrition_toujeo_pct;

create table ty33_patient_attrition_toujeo_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_toujeo)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_toujeo_pct
;

drop table if exists ty33_patient_attrition_other;

create table ty33_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_other;

drop table if exists ty33_patient_attrition_other_pct;

create table ty33_patient_attrition_other_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_other)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_other_pct
;

drop table if exists ty33_patient_attrition_gla100;

create table ty33_patient_attrition_gla100 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_gla100;

drop table if exists ty33_patient_attrition_gla100_pct;

create table ty33_patient_attrition_gla100_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_gla100)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_gla100_pct
;

drop table if exists ty33_patient_attrition_detemir;

create table ty33_patient_attrition_detemir as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_detemir;

drop table if exists ty33_patient_attrition_detemir_pct;

create table ty33_patient_attrition_detemir_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_detemir)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_detemir_pct
;

drop table if exists ty33_patient_attrition_nph;

create table ty33_patient_attrition_nph as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm)
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_nph;

drop table if exists ty33_patient_attrition_nph_pct;

create table ty33_patient_attrition_nph_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_nph)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_nph_pct
;

drop table if exists ty33_pat_all_enrol_demog;

create table ty33_pat_all_enrol_demog as
select distinct a.*, b.eligeff, b.eligend, b.bus, b.division,b.product
from (select *, 1 as fl_study_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and isnull(dt_1st_t1dm) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))) a left join ty19_ses_2208_mem_enrol b
on a.patid=b.patid and a.dt_rx_index between b.eligeff and b.eligend
order by a.patid
;

select * from ty33_pat_all_enrol_demog;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol_demog
;

select patid, dt_1st_t2dm, dt_1st_t1dm, dt_rx_index, dt_1st_toujeo, index_group, age_index, enrlstdt, dt_last_glp1_bl, dt_last_a1c_bl, dt_1st_a1c_fu,dt_1st_glp1_fu,dt_last_insulin_bl,dt_rx_other_insulin
from ty33_pat_all_enrol
where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
      and dt_dx_index between '2015-01-01' and '2021-12-31'
--    and age_index>=18
--    and dt_rx_index>=date_add(enrlstdt,180)
--    and isnotnull(dt_last_glp1_bl)
--    and isnotnull(dt_last_a1c_bl)
--    and isnotnull(dt_1st_a1c_fu)
--    and isnotnull(dt_1st_glp1_fu)
--    and isnull(dt_1st_t1dm)
--    and isnull(dt_last_insulin_bl)
      and dt_1st_toujeo=dt_rx_other_insulin
order by patid
;


-- COMMAND ----------

drop table if exists ty33_patient_attrition_toujeo;

create table ty33_patient_attrition_toujeo as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_toujeo;

drop table if exists ty33_patient_attrition_toujeo_pct;

create table ty33_patient_attrition_toujeo_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_toujeo)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_toujeo_pct
;


-- COMMAND ----------

drop table if exists ty19_lab_glucose;

create table ty19_lab_glucose as
select distinct *
from ty19_ses_2208_lab_result
where lcase(tst_desc) like '%glucose%'
order by patid, fst_dt
;

select * from ty19_lab_glucose;


-- COMMAND ----------

select * from ty19_lab_glucose
where isnotnull(rslt_txt)
;

-- COMMAND ----------

drop table if exists ty19_lab_glucose_loinc_value;

create table ty19_lab_glucose_loinc_value as
select *, rslt_nbr as value
from ty19_lab_glucose
where lcase(rslt_unit_nm) like '%mg/dl%' and rslt_nbr>0
;

select TST_DESC, count(*), min(rslt_nbr), max(rslt_nbr)
from ty19_lab_glucose_loinc_value
group by TST_DESC
order by TST_DESC
;

-- COMMAND ----------

drop table if exists ty33_patient_attrition_other;

create table ty33_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_other;

drop table if exists ty33_patient_attrition_other_pct;

create table ty33_patient_attrition_other_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_other)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_other_pct
;

drop table if exists ty33_patient_attrition_gla100;

create table ty33_patient_attrition_gla100 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_gla100;

drop table if exists ty33_patient_attrition_gla100_pct;

create table ty33_patient_attrition_gla100_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_gla100)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_gla100_pct
;

drop table if exists ty33_patient_attrition_detemir;

create table ty33_patient_attrition_detemir as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_detemir;

drop table if exists ty33_patient_attrition_detemir_pct;

create table ty33_patient_attrition_detemir_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_detemir)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_detemir_pct
;

drop table if exists ty33_patient_attrition_nph;

create table ty33_patient_attrition_nph as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_nph;

drop table if exists ty33_patient_attrition_nph_pct;

create table ty33_patient_attrition_nph_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_nph)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_nph_pct
;

drop table if exists ty33_pat_all_enrol_demog;

create table ty33_pat_all_enrol_demog as
select distinct a.*, b.eligeff, b.eligend, b.bus, b.division,b.product
from (select *, 1 as fl_study_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))) a left join ty19_ses_2208_mem_enrol b
on a.patid=b.patid and a.dt_rx_index between b.eligeff and b.eligend
order by a.patid
;

select * from ty33_pat_all_enrol_demog;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol_demog
;


-- COMMAND ----------

select * from ty19_ses_2208_med_diag;

-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_hypo;

create table ty33_fu_6m_dx_hypo as
select distinct *
from (select a.patid, a.fst_dt, a.diag, b.dt_rx_index from ty19_ses_2208_med_diag where diag rlike '^(2510|2511|2512|2703)' a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
order by patid
;

select * from ty33_fu_6m_dx_hypo;

-- COMMAND ----------

drop table if exists ty33_dx_hypo;

create table ty33_dx_hypo as
select distinct patid, fst_dt, diag,
        case when diag rlike '^(2510|2511|2512|2703|E0864|E08641|E08649|E0964|E09641|E09649|E1064|E10641|E10649|E1164|E11641|E11649|E1364|E13641)' then 'Hypo'
             when diag like '2508%' then 'Hypo_inc'
             when diag rlike '^(2598|2727|5238|5239,681|682|6929,7071|7072|7073|7074|7075|7076|7077|7078|7079|7093|7300|7301|7302|7318)' then 'Hypo_exc'
             else null end as hype_type
from ty19_ses_2208_med_diag
where (diag rlike '^(2510|2511|2512|2703|E0864|E08641|E08649|E0964|E09641|E09649|E1064|E10641|E10649|E1164|E11641|E11649|E1364|E13641)' or
       diag rlike '^(2508|2598|2727|5238|5239,681|682|6929,7071|7072|7073|7074|7075|7076|7077|7078|7079|7093|7300|7301|7302|7318)')
       and fst_dt>='2014-07-01'
order by patid, fst_dt
;

select * from ty33_dx_hypo;


-- COMMAND ----------

select hype_type, count(*)
from ty33_dx_hypo
group by hype_type
;

-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_hypo;

create table ty33_fu_6m_dx_hypo as
select distinct a.patid, a.dt_rx_index, min(b.fst_dt) as dt_rx_hypo, min(c.fst_dt) as dt_rx_hypo_inc, min(d.fst_dt) as dt_rx_hypo_exc
        , case when isnotnull(min(b.fst_dt)) then min(b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then min(c.fst_dt)
               else null end as dt_rx_hypo_fu_6m
from ty33_pat_all_enrol_demog a left join ty33_dx_hypo b on a.patid=b.patid and b.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and b.hype_type='Hypo'
                                left join ty33_dx_hypo c on a.patid=c.patid and c.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and c.hype_type='Hypo_inc'
                                left join ty33_dx_hypo d on a.patid=d.patid and d.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and d.hype_type='Hypo_exc'
group by a.patid, a.dt_rx_index
order by a.patid
;

select * from ty33_fu_6m_dx_hypo;

-- COMMAND ----------

select * from ty33_fu_6m_dx_hypo
where isnotnull(dt_rx_hypo_inc);

-- COMMAND ----------

drop table if exists ty33_fu_6m_lab_glucose;

create table ty33_fu_6m_lab_glucose as
select distinct patid, fst_dt as dt_glucose_last_fu_6m, value as fu_6m_glucose_last, rank
from (select a.patid, a.fst_dt, a.value, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty19_lab_glucose_loinc_value a join ty33_pat_all_enrol_demog b
        on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid
;

select * from ty33_fu_6m_lab_glucose;

-- COMMAND ----------

drop table if exists ty33_fu_6m_a1c;

create table ty33_fu_6m_a1c as
select distinct patid, fst_dt as dt_last_a1c_fu_6m, value as fu_6m_a1c_last, rank
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210))
where rank<=1
order by patid
;

select * from ty33_fu_6m_a1c;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_fu_6m) as dt_a1c_start, max(dt_last_a1c_fu_6m) as dt_a1c_stop
from ty33_fu_6m_a1c
;

drop table if exists ty33_bl_6m_a1c;

create table ty33_bl_6m_a1c as
select distinct patid, fst_dt as dt_last_a1c_bl_6m, value as bl_6m_a1c_last, rank
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid
;

select * from ty33_bl_6m_a1c;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_bl_6m) as dt_a1c_start, max(dt_last_a1c_bl_6m) as dt_a1c_stop
from ty33_bl_6m_a1c
;



-- COMMAND ----------

drop table if exists ty33_fu_6m_med;

create table ty33_fu_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_fu_6m_med
;

drop table if exists ty33_fu_6m_med_dm;

create table ty33_fu_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_fu_6m_med a join (select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                            union
                            select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                            ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med_dm;

drop table if exists ty33_fu_6m_med_dm_max;

create table ty33_fu_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_fu_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_fu_6m_med_dm_max;

drop table if exists ty33_fu_6m_rx;

create table ty33_fu_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx;

drop table if exists ty33_fu_6m_rx_anti_dm;

create table ty33_fu_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_fu_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx_anti_dm;

drop table if exists ty33_pat_fu_6m_cost;

create table ty33_pat_fu_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.fu_6m_cost_med) as fu_6m_cost_med, max(b.fu_6m_cost_med_inp) as fu_6m_cost_med_inp, max(c.fu_6m_cost_med_er) as fu_6m_cost_med_er, max(d.fu_6m_cost_med_dm) as fu_6m_cost_med_dm
        , max(e.fu_6m_cost_med_dm_inp) as fu_6m_cost_med_dm_inp, max(f.fu_6m_cost_med_dm_er) as fu_6m_cost_med_dm_er, max(g.fu_6m_cost_rx) as fu_6m_cost_rx, max(h.fu_6m_cost_rx_anti_dm) as fu_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as fu_6m_cost_med from ty33_fu_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_inp from ty33_fu_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_er from ty33_fu_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm from ty33_fu_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_inp from ty33_fu_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_er from ty33_fu_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx from ty33_fu_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx_anti_dm from ty33_fu_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_cost
;

drop table if exists ty33_bl_6m_med;

create table ty33_bl_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med;

drop table if exists ty33_bl_6m_med_dm;

create table ty33_bl_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_bl_6m_med a join (select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med_dm;

drop table if exists ty33_bl_6m_med_dm_max;

create table ty33_bl_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_bl_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_bl_6m_med_dm_max;

drop table if exists ty33_bl_6m_rx;

create table ty33_bl_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx;

drop table if exists ty33_bl_6m_rx_anti_dm;

create table ty33_bl_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_bl_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx_anti_dm;

drop table if exists ty33_pat_bl_6m_cost;

create table ty33_pat_bl_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.bl_6m_cost_med) as bl_6m_cost_med, max(b.bl_6m_cost_med_inp) as bl_6m_cost_med_inp, max(c.bl_6m_cost_med_er) as bl_6m_cost_med_er, max(d.bl_6m_cost_med_dm) as bl_6m_cost_med_dm
        , max(e.bl_6m_cost_med_dm_inp) as bl_6m_cost_med_dm_inp, max(f.bl_6m_cost_med_dm_er) as bl_6m_cost_med_dm_er, max(g.bl_6m_cost_rx) as bl_6m_cost_rx, max(h.bl_6m_cost_rx_anti_dm) as bl_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as bl_6m_cost_med from ty33_bl_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_inp from ty33_bl_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_er from ty33_bl_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm from ty33_bl_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_inp from ty33_bl_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_er from ty33_bl_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx from ty33_bl_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx_anti_dm from ty33_bl_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_cost
;


-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_comorb1;

create table ty33_fu_6m_dx_comorb1 as
select distinct patid, dx_name, fst_dt as dt_last_dx_fu_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_11_16 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, dx_name
;

select * from ty33_fu_6m_dx_comorb1;

drop table if exists ty33_fu_6m_dx_comorb2;

create table ty33_fu_6m_dx_comorb2 as
select distinct patid, dx_name, fst_dt as dt_last_dx_fu_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_17_22 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, dx_name
;

select * from ty33_fu_6m_dx_comorb2;

drop table if exists ty33_fu_6m_dx_comorb;

create table ty33_fu_6m_dx_comorb as
select distinct coalesce(a.patid, b.patid) as patid, coalesce(a.dx_name, b.dx_name) as dx_name, coalesce(a.Disease, b.Disease) as Disease, coalesce(a.weight, b.weight) as weight
        , coalesce(a.weight_old, b.weight_old) as weight_old, greatest(a.dt_last_dx_fu_6m, b.dt_last_dx_fu_6m) as dt_last_dx_fu_6m
from ty33_fu_6m_dx_comorb1 a full join ty33_fu_6m_dx_comorb2 b
on a.patid=b.patid and a.dx_name=b.dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_fu_6m) as dt_dx_start, max(dt_last_dx_fu_6m) as dt_dx_stop
from ty33_fu_6m_dx_comorb
group by dx_name
order by dx_name
;

drop table if exists ty33_bl_6m_dx_comorb1;

create table ty33_bl_6m_dx_comorb1 as
select distinct patid, dx_name, fst_dt as dt_last_dx_bl_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_11_16 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, dx_name
;

select * from ty33_bl_6m_dx_comorb1;

drop table if exists ty33_bl_6m_dx_comorb2;

create table ty33_bl_6m_dx_comorb2 as
select distinct patid, dx_name, fst_dt as dt_last_dx_bl_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_17_22 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, dx_name
;

select * from ty33_bl_6m_dx_comorb2;

drop table if exists ty33_bl_6m_dx_comorb;

create table ty33_bl_6m_dx_comorb as
select distinct coalesce(a.patid, b.patid) as patid, coalesce(a.dx_name, b.dx_name) as dx_name, coalesce(a.Disease, b.Disease) as Disease, coalesce(a.weight, b.weight) as weight
        , coalesce(a.weight_old, b.weight_old) as weight_old, greatest(a.dt_last_dx_bl_6m, b.dt_last_dx_bl_6m) as dt_last_dx_bl_6m
from ty33_bl_6m_dx_comorb1 a full join ty33_bl_6m_dx_comorb2 b
on a.patid=b.patid and a.dx_name=b.dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_bl_6m) as dt_dx_start, max(dt_last_dx_bl_6m) as dt_dx_stop
from ty33_bl_6m_dx_comorb
group by dx_name
order by dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_bl_6m) as dt_dx_start, max(dt_last_dx_bl_6m) as dt_dx_stop
from ty33_bl_6m_dx_comorb
group by dx_name
order by dx_name
;



-- COMMAND ----------

drop table if exists ty33_fu_6m_inp_er;

create table ty33_fu_6m_inp_er as
select distinct a.patid, a.clmid, a.fst_dt, a.lst_dt, a.pos, a.proc_cd, a.conf_id, b.dt_rx_index
from (select * from ty19_ses_2208_med_claim where pos in ('21', '23')) a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_inp_er;

drop table if exists ty33_fu_6m_inp_er_dm;

create table ty33_fu_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_fu_6m_inp_er a join (select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_inp_er_dm;

drop table if exists ty33_pat_fu_6m_inp_er;

create table ty33_pat_fu_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.fu_6m_inp) as fu_6m_inp, max(e.fu_6m_inp_n) as fu_6m_inp_n, max(e.fu_6m_inp_days) as fu_6m_inp_days, max(b.fu_6m_er) as fu_6m_er, max(f.fu_6m_er_n) as fu_6m_er_n
        , max(c.fu_6m_inp_dm) as fu_6m_inp_dm, max(g.fu_6m_inp_n_dm) as fu_6m_inp_n_dm, max(g.fu_6m_inp_days_dm) as fu_6m_inp_days_dm
        , max(d.fu_6m_er_dm) as fu_6m_er_dm, max(h.fu_6m_er_n_dm) as fu_6m_er_n_dm
from (select distinct patid, 1 as fu_6m_inp from ty33_fu_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as fu_6m_er from ty33_fu_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as fu_6m_inp_dm from ty33_fu_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as fu_6m_er_dm from ty33_fu_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days from ty33_fu_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n from ty33_fu_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days_dm from ty33_fu_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n_dm from ty33_fu_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_inp_er
;

select a.*, b.n_obs
from ty33_pat_fu_6m_inp_er a join (select patid1, count(*) as n_obs from ty33_pat_fu_6m_inp_er group by patid1) b
on a.patid1=b.patid1
where b.n_obs>1
order by a.patid1
;

drop table if exists ty33_bl_6m_inp_er;

create table ty33_bl_6m_inp_er as
select distinct a.patid, a.clmid, a.fst_dt, a.lst_dt, a.pos, a.proc_cd, a.conf_id, b.dt_rx_index
from (select * from ty19_ses_2208_med_claim where pos in ('21', '23')) a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er;

drop table if exists ty33_bl_6m_inp_er_dm;

create table ty33_bl_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_bl_6m_inp_er a join (select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er_dm;

drop table if exists ty33_pat_bl_6m_inp_er;

create table ty33_pat_bl_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.bl_6m_inp) as bl_6m_inp, max(e.bl_6m_inp_n) as bl_6m_inp_n, max(e.bl_6m_inp_days) as bl_6m_inp_days, max(b.bl_6m_er) as bl_6m_er, max(f.bl_6m_er_n) as bl_6m_er_n
        , max(c.bl_6m_inp_dm) as bl_6m_inp_dm, max(g.bl_6m_inp_n_dm) as bl_6m_inp_n_dm, max(g.bl_6m_inp_days_dm) as bl_6m_inp_days_dm
        , max(d.bl_6m_er_dm) as bl_6m_er_dm, max(h.bl_6m_er_n_dm) as bl_6m_er_n_dm
from (select distinct patid, 1 as bl_6m_inp from ty33_bl_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as bl_6m_er from ty33_bl_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as bl_6m_inp_dm from ty33_bl_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as bl_6m_er_dm from ty33_bl_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days from ty33_bl_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n from ty33_bl_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days_dm from ty33_bl_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n_dm from ty33_bl_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_inp_er
;

select a.*, b.n_obs
from ty33_pat_bl_6m_inp_er a join (select patid1, count(*) as n_obs from ty33_pat_bl_6m_inp_er group by patid1) b
on a.patid1=b.patid1
where b.n_obs>1
order by a.patid1
;


-- COMMAND ----------

drop table if exists ty33_fu_6m_rx;

create table ty33_fu_6m_rx as
select distinct patid, rx_type, fill_dt as dt_last_rx_fu_6m, dt_rx_index, gnrc_nm, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.rx_type order by a.fill_dt desc, a.ndc) as rank from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fill_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, rx_type
;

select * from ty33_fu_6m_rx;

select rx_type, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_rx_fu_6m) as dt_rx_start, max(dt_last_rx_fu_6m) as dt_rx_stop
from ty33_fu_6m_rx
group by rx_type
order by rx_type
;

drop table if exists ty33_bl_6m_rx;

create table ty33_bl_6m_rx as
select distinct patid, rx_type, fill_dt as dt_last_rx_bl_6m, dt_rx_index, gnrc_nm, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.rx_type order by a.fill_dt desc, a.ndc) as rank from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, rx_type
;

select * from ty33_bl_6m_rx;

select rx_type, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_rx_bl_6m) as dt_rx_start, max(dt_last_rx_bl_6m) as dt_rx_stop
from ty33_bl_6m_rx
group by rx_type
order by rx_type
;



-- COMMAND ----------

drop table if exists ty33_index_social;

create table ty33_index_social as
select distinct a.patid, d_education_level_code,d_fed_poverty_status_code,d_home_ownership_code,d_household_income_range_code,d_networth_range_code,d_occupation_type_code,d_race_code,num_adults,num_child, dt_rx_index
from ty19_ses_2208_Socioeconomic a join ty33_pat_all_enrol_demog b
on a.patid=b.patid
order by a.patid
;

select * from ty33_index_social;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_rx_index) as dt_start, max(dt_rx_index) as dt_stop
from ty33_index_social
;



-- COMMAND ----------

drop table if exists ty33_patient_attrition;

create table ty33_patient_attrition as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition;

drop table if exists ty33_patient_attrition_pct;

create table ty33_patient_attrition_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_pct
;


-- COMMAND ----------

select * from ty33_patient_attrition_nph;

-- COMMAND ----------



-- COMMAND ----------

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol_demog
;

-- COMMAND ----------

select count(*) as n_pat, 1 as fl_study_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_toujeo_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_toujeo_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_other_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_other_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_gla100_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_gla100_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_detemir_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_detemir_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_patient_attrition_nph_pct")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_patient_attrition_nph_pct")
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

select division, count(*)
from ty33_pat_all_enrol_demog
group by division
order by division
;


-- COMMAND ----------

drop table if exists ty33_pat_all_enrol_demog;

create table ty33_pat_all_enrol_demog as
select distinct a.*, b.eligeff, b.eligend, b.bus, b.division,b.product
        , case when b.division='EAST NORTH CENTRAL' then 'Midwest'
               when b.division='EAST SOUTH CENTRAL' then 'South'
               when b.division='MIDDLE ATLANTIC'    then 'Northeast'
               when b.division='MOUNTAIN'           then 'West'
               when b.division='NEW ENGLAND'        then 'Northeast'
               when b.division='PACIFIC'            then 'West'
               when b.division='SOUTH ATLANTIC'     then 'South'
               when b.division='WEST NORTH CENTRAL' then 'Midwest'
               when b.division='WEST SOUTH CENTRAL' then 'South'
               else null end as region
from (select *, 1 as fl_study_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))) a left join ty19_ses_2208_mem_enrol b
on a.patid=b.patid and a.dt_rx_index between b.eligeff and b.eligend
order by a.patid
;
select * from ty33_pat_all_enrol_demog;


-- COMMAND ----------

select distinct rx_type, gnrc_nm, brnd_nm
from ty19_rx_anti_dm
where lcase(rx_type) like '%glp1%'
order by rx_type, gnrc_nm, brnd_nm
;

-- COMMAND ----------

drop table if exists ty33_pat_dx_t2dm;

create table ty33_pat_dx_t2dm as
select distinct *
from (select *, dense_rank() OVER (PARTITION BY patid ORDER BY fst_dt, diag_position, clmid, pat_planid, diag, description) as rank
      from (select * from ty19_dx_subset_17_22 where dx_name='T2DM'
            union
            select * from ty19_dx_subset_11_16 where fst_dt>='2014-07-01' and dx_name='T2DM'
            ))
order by patid
;

select dx_name, count(*) as n_obs, count(distinct patid) as n_pat, min(fst_dt) as dt_t2dm_start, max(fst_dt) as dt_t2dm_stop
from ty33_pat_dx_t2dm
group by dx_name
order by dx_name
;

select a.*, b.n_obs
from ty33_pat_dx_t2dm a join (select patid, count(*) as n_obs from ty33_pat_dx_t2dm group by patid) b
on a.patid=b.patid
where b.n_obs>1
order by patid
;

drop table if exists ty33_pat_dx_T1DM;

create table ty33_pat_dx_T1DM as
select distinct *
from (select *, dense_rank() OVER (PARTITION BY patid ORDER BY fst_dt, diag_position, clmid, pat_planid, diag, description) as rank
      from (select * from ty19_dx_subset_17_22 where dx_name in ('T1DM', 'DM_2nd')
            union
            select * from ty19_dx_subset_11_16 where fst_dt>='2014-07-01' and dx_name in ('T1DM', 'DM_2nd')
            ))
order by patid
;

select dx_name, count(*) as n_obs, count(distinct patid) as n_pat, min(fst_dt) as dt_t1dm_start, max(fst_dt) as dt_t1dm_stop
from ty33_pat_dx_T1DM
group by dx_name
order by dx_name
;

drop table if exists ty33_pat_rx_basal_nph_glp1;

create table ty33_pat_rx_basal_nph_glp1 as
select distinct patid, charge,clmid,copay,days_sup,deduct,dispfee,fill_dt,quantity,specclss,std_cost,std_cost_yr,strength,brnd_nm,gnrc_nm,ndc,rx_type
       , case when lcase(brnd_nm) like '%toujeo solostar%' then 'Toujeo'
              when lcase(gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(brnd_nm) not like '%toujeo%' then 'Gla-100'
              when lcase(gnrc_nm) like '%detemir%' then 'Detemir'
              when lcase(gnrc_nm) like '%nph%' then 'NPH'
              when lcase(rx_type) like '%glp1%' then 'GLP1'
                   else rx_type end as rx_type2
from ty19_rx_anti_dm
where fill_dt>='2014-07-01' and (lcase(rx_type) in ('basal', 'bolus', 'premix')
        or (lcase(brnd_nm) in ('trulicity','ozempic','bydureon','bydureon bcise','bydureon pen')))
order by patid, fill_dt
;

select distinct rx_type, gnrc_nm, brnd_nm
from ty19_rx_anti_dm
where lcase(rx_type) like '%glp1%'
order by rx_type, gnrc_nm, brnd_nm
;

select rx_type2, rx_type, gnrc_nm,brnd_nm, min(fill_dt) as dt_rx_start, max(fill_dt) as dt_rx_end
from ty33_pat_rx_basal_nph_glp1
group by rx_type2, rx_type, gnrc_nm,brnd_nm
order by rx_type2, rx_type, gnrc_nm,brnd_nm
;

drop table if exists ty33_pat_dx_rx;

create table ty33_pat_dx_rx as
select distinct a.patid, min(a.dt_1st_t2dm) as dt_1st_t2dm, min(a.n_t2dm) as n_t2dm, min(b.dt_1st_t1dm) as dt_1st_t1dm, min(b.n_t1dm) as n_t1dm, min(c.dt_1st_toujeo) as dt_1st_toujeo
        , min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) as dt_rx_other_insulin
        , min(d.dt_1st_gla_100) as dt_1st_gla_100, min(e.dt_1st_detemir) as dt_1st_detemir, min(f.dt_1st_nph) as dt_1st_nph
        , case when isnotnull(min(c.dt_1st_toujeo)) then min(c.dt_1st_toujeo)
               else min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph)) end as dt_rx_index
        , case when isnotnull(min(c.dt_1st_toujeo)) then 'Toujeo'
               when isnotnull(min(least(d.dt_1st_gla_100, e.dt_1st_detemir, f.dt_1st_nph))) then 'Other long-acting BIs'
               else null end as index_group
from (select distinct patid, min(fst_dt) as dt_1st_t2dm, count(distinct fst_dt) as n_t2dm from ty33_pat_dx_t2dm group by patid) a
      left join (select distinct patid, min(fst_dt) as dt_1st_t1dm, count(distinct fst_dt) as n_t1dm from ty33_pat_dx_t1dm group by patid) b on a.patid=b.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_toujeo from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Toujeo') group by patid) c on a.patid=c.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_gla_100 from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Gla-100') group by patid) d on a.patid=d.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_detemir from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('Detemir') group by patid) e on a.patid=e.patid
      left join (select distinct patid, min(fill_dt) as dt_1st_nph from ty33_pat_rx_basal_nph_glp1 where fill_dt>='2015-01-01' and rx_type2 in ('NPH') group by patid) f on a.patid=f.patid
group by a.patid
order by a.patid
;

select count(*) as n_obs, count(distinct patid) as n_pat, min(dt_1st_t2dm) as dt_d2dm_start, max(dt_1st_t2dm) as dt_d2dm_end
from ty33_pat_dx_rx
where isnotnull(dt_rx_index)
;

select index_group, count(*) as n_obs, count(distinct patid) as n_pat, min(dt_rx_index) as dt_rx_start, max(dt_rx_index) as dt_rx_end
from ty33_pat_dx_rx
group by index_group
;

drop table if exists ty33_pat_rx_basal_nph_glp1_index;

create table ty33_pat_rx_basal_nph_glp1_index as
select distinct a.*, b.dt_rx_index
from ty33_pat_rx_basal_nph_glp1 a left join ty33_pat_dx_rx b
on a.patid=b.patid
order by a.patid, a.fill_dt
;

select * from ty33_pat_rx_basal_nph_glp1_index;

drop table if exists ty33_lab_a1c_index;

create table ty33_lab_a1c_index as
select a.*, b.dt_rx_index
from ty19_lab_a1c_loinc_value a join ty33_pat_dx_rx b
on a.patid=b.patid
where isnotnull(b.dt_rx_index) and a.value between 3 and 15
order by a.patid, a.fst_dt
;

select * from ty33_lab_a1c_index;

drop table if exists ty33_glp1_a1c_bl_fu;

create table ty33_glp1_a1c_bl_fu as
select distinct a.patid as patid1, max(b.dt_last_glp1_bl) as dt_last_glp1_bl, max(c.dt_last_a1c_bl) as dt_last_a1c_bl
        , min(d.dt_1st_a1c_fu) as dt_1st_a1c_fu, min(e.dt_1st_glp1_fu) as dt_1st_glp1_fu, max(f.dt_last_insulin_bl) as dt_last_insulin_bl
from (select patid, dt_rx_index from ty33_pat_dx_rx where isnotnull(dt_rx_index)) a
     left join (select distinct patid, max(fill_dt) as dt_last_glp1_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt between date_sub(dt_rx_index,540) and date_sub(dt_rx_index,1) group by patid) b on a.patid=b.patid
     left join (select distinct patid, max(fst_dt) as dt_last_a1c_bl from ty33_lab_a1c_index where fst_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) c on a.patid=c.patid
     left join (select distinct patid, min(fst_dt) as dt_1st_a1c_fu from ty33_lab_a1c_index where fst_dt between date_add(dt_rx_index,90) and date_add(dt_rx_index,210) group by patid) d on a.patid=d.patid
     left join (select distinct patid, min(fill_dt) as dt_1st_glp1_fu from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) in ('glp1') and fill_dt >= dt_rx_index group by patid) e on a.patid=e.patid
     left join (select distinct patid, max(fill_dt) as dt_last_insulin_bl from ty33_pat_rx_basal_nph_glp1_index where lcase(rx_type) not in ('glp1') and fill_dt between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1) group by patid) f on a.patid=f.patid
group by patid1
order by patid1
;

select * from ty33_glp1_a1c_bl_fu;

drop table if exists ty33_pat_all_enrol;

create table ty33_pat_all_enrol as
select distinct a.*, case when index_group='Toujeo' then index_group
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_gla_100 then 'Gla-100'
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_detemir then 'Detemir'
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_nph then 'NPH'
                          else null end as index_group2
                   , case when index_group='Toujeo' then dt_rx_index
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_gla_100 then dt_1st_gla_100
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_detemir then dt_1st_detemir
                          when index_group!='Toujeo' and dt_rx_other_insulin=dt_1st_nph then dt_1st_nph
                          else null end as dt_rx_index2
                   , b.*, c.eligeff as enrlstdt, c.eligend as enrlendt, c.gdr_cd, c.yrdob, year(a.dt_rx_index)-c.yrdob as age_index
from ty33_pat_dx_rx a left join ty33_glp1_a1c_bl_fu b on a.patid=b.patid1
                      left join ty19_ses_2208_mem_conti c on a.patid=c.patid and a.dt_rx_index between c.eligeff and c.eligend
order by a.patid
;

select * from ty33_pat_all_enrol;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol
;

select patid,dt_1st_toujeo,dt_rx_other_insulin,dt_1st_gla_100,dt_1st_detemir,dt_1st_nph,dt_rx_index,index_group, dt_rx_index2,index_group2
from ty33_pat_all_enrol
where isnotnull(dt_rx_index)
;

drop table if exists ty33_patient_attrition;

create table ty33_patient_attrition as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition;

drop table if exists ty33_patient_attrition_pct;

create table ty33_patient_attrition_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_pct
;

drop table if exists ty33_patient_attrition_toujeo;

create table ty33_patient_attrition_toujeo as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Toujeo' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_toujeo;

drop table if exists ty33_patient_attrition_toujeo_pct;

create table ty33_patient_attrition_toujeo_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_toujeo)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_toujeo_pct
;

drop table if exists ty33_patient_attrition_other;

create table ty33_patient_attrition_other as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group='Other long-acting BIs' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo)) and dt_rx_index<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_other;

drop table if exists ty33_patient_attrition_other_pct;

create table ty33_patient_attrition_other_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_other)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_other_pct
;

drop table if exists ty33_patient_attrition_gla100;

create table ty33_patient_attrition_gla100 as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Gla-100' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_gla_100=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_gla_100)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_gla100;

drop table if exists ty33_patient_attrition_gla100_pct;

create table ty33_patient_attrition_gla100_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_gla100)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_gla100_pct
;

drop table if exists ty33_patient_attrition_detemir;

create table ty33_patient_attrition_detemir as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='Detemir' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_detemir=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_detemir)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_detemir;

drop table if exists ty33_patient_attrition_detemir_pct;

create table ty33_patient_attrition_detemir_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_detemir)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_detemir_pct
;

drop table if exists ty33_patient_attrition_nph;

create table ty33_patient_attrition_nph as
select ' 1. Diagnosis of T2D according to ICD-9/10-CM codes at any point during 7/1/2014 and 6/30/2022' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30'
union
select ' 2. Have at least one pharmacy fills of Gla-300 or other long-acting BIs during 1/1/2015 and 12/31/2021' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31'
union
select ' 3. Age 18 and above on index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18
union
select ' 4. At least 6 months continuous medical and pharmacy eligibility prior to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180)
union
select ' 5. At least one weekly GLP-1 RA during the extended baseline period (540 days prior to index date)' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl)
union
select ' 6. Have at least one valid HbA1c during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl)
union
select ' 7. Have at least one valid HbA1c during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
union
select ' 8. Have at least one same weekly GLP-1 RA during the follow-up period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu)
union
select ' 9. Those without any T1D diagnoses identified' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm))
union
select '10. Those without prior pharmacy fills of insulins (rapid-acting, short-acting, premix, FRC, long-acting, longer-acting BIs) during the baseline period' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl)
union
select '11. Those without pharmacy fills of more than one basal insulin on the index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph))
union
select '12. At least 6 months continuous medical and pharmacy eligibility post to index date' as Step, count(distinct patid) as n_pat from ty33_pat_all_enrol where index_group2='NPH' and dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index2 between '2015-01-01' and '2021-12-31' and age_index>=18 and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu) and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_nph=dt_1st_toujeo and isnotnull(dt_1st_toujeo) and isnotnull(dt_1st_nph)) and dt_rx_index2<=date_sub(enrlendt,179)
order by Step
;

select * from ty33_patient_attrition_nph;

drop table if exists ty33_patient_attrition_nph_pct;

create table ty33_patient_attrition_nph_pct as
select distinct *, round(100*n_pat/pre_n,2) as pct
from (select *, lag(n_pat) over (order by step) as pre_n from ty33_patient_attrition_nph)
order by step
;

select Step, format_number(n_pat,0) as N, pct
from ty33_patient_attrition_nph_pct
;

drop table if exists ty33_pat_all_enrol_demog;

create table ty33_pat_all_enrol_demog as
select distinct a.*, b.eligeff, b.eligend, b.bus, b.division,b.product
        , case when b.division='EAST NORTH CENTRAL' then 'Midwest'
               when b.division='EAST SOUTH CENTRAL' then 'South'
               when b.division='MIDDLE ATLANTIC'    then 'Northeast'
               when b.division='MOUNTAIN'           then 'West'
               when b.division='NEW ENGLAND'        then 'Northeast'
               when b.division='PACIFIC'            then 'West'
               when b.division='SOUTH ATLANTIC'     then 'South'
               when b.division='WEST NORTH CENTRAL' then 'Midwest'
               when b.division='WEST SOUTH CENTRAL' then 'South'
               else null end as region
from (select *, 1 as fl_study_pat from ty33_pat_all_enrol where dt_1st_t2dm between '2014-07-01' and '2022-06-30' and dt_rx_index between '2015-01-01' and '2021-12-31' and age_index>=18
        and dt_rx_index>=date_add(enrlstdt,180) and isnotnull(dt_last_glp1_bl) and isnotnull(dt_last_a1c_bl) and isnotnull(dt_1st_a1c_fu)
        and isnotnull(dt_1st_glp1_fu) and (isnull(dt_1st_t1dm) or (isnotnull(n_t1dm) and n_t2dm>2*n_t1dm)) and isnull(dt_last_insulin_bl) and not(dt_1st_toujeo=dt_rx_other_insulin
        and isnotnull(dt_rx_other_insulin) and isnotnull(dt_1st_toujeo))) a left join ty19_ses_2208_mem_enrol b
on a.patid=b.patid and a.dt_rx_index between b.eligeff and b.eligend
order by a.patid
;

select * from ty33_pat_all_enrol_demog;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_pat_all_enrol_demog
;


-- COMMAND ----------

select distinct dx_name
from ty19_dx_subset_11_16
order by dx_name;

-- COMMAND ----------

drop table if exists ty33_dx_hypo;

create table ty33_dx_hypo as
select distinct patid, fst_dt, diag,
        case when diag rlike '^(2510|2511|2512|2703|E0864|E08641|E08649|E0964|E09641|E09649|E1064|E10641|E10649|E1164|E11641|E11649|E1364|E13641)' then 'Hypo'
             when diag like '2508%' then 'Hypo_inc'
             when diag rlike '^(2598|2727|5238|5239,681|682|6929,7071|7072|7073|7074|7075|7076|7077|7078|7079|7093|7300|7301|7302|7318)' then 'Hypo_exc'
             else null end as hypo_type
from ty19_ses_2208_med_diag
where (diag rlike '^(2510|2511|2512|2703|E0864|E08641|E08649|E0964|E09641|E09649|E1064|E10641|E10649|E1164|E11641|E11649|E1364|E13641)' or
       diag rlike '^(2508|2598|2727|5238|5239,681|682|6929,7071|7072|7073|7074|7075|7076|7077|7078|7079|7093|7300|7301|7302|7318)')
       and fst_dt>='2014-07-01'
order by patid, fst_dt
;

select * from ty33_dx_hypo;

select hypo_type, count(*)
from ty33_dx_hypo
group by hypo_type
;

drop table if exists ty33_fu_6m_dx_hypo;

create table ty33_fu_6m_dx_hypo as
select distinct a.patid, a.dt_rx_index, min(b.fst_dt) as dt_dx_hypo, min(c.fst_dt) as dt_dx_hypo_inc, min(d.fst_dt) as dt_dx_hypo_exc
        , case when isnotnull(min(b.fst_dt)) then min(b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then min(c.fst_dt)
               else null end as dt_dx_hypo_fu_6m
        , case when isnotnull(min(b.fst_dt)) then count(distinct b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then count(distinct c.fst_dt)
               else null end as n_dx_hypo_fu_6m
from ty33_pat_all_enrol_demog a left join ty33_dx_hypo b on a.patid=b.patid and b.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and b.hypo_type='Hypo'
                                left join ty33_dx_hypo c on a.patid=c.patid and c.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and c.hypo_type='Hypo_inc'
                                left join ty33_dx_hypo d on a.patid=d.patid and d.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and d.hypo_type='Hypo_exc'
group by a.patid, a.dt_rx_index
order by a.patid
;

select * from ty33_fu_6m_dx_hypo;



-- COMMAND ----------

drop table if exists ty33_bl_6m_dx_hypo;

create table ty33_bl_6m_dx_hypo as
select distinct a.patid, a.dt_rx_index, min(b.fst_dt) as dt_dx_hypo, min(c.fst_dt) as dt_dx_hypo_inc, min(d.fst_dt) as dt_dx_hypo_exc
        , case when isnotnull(min(b.fst_dt)) then min(b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then min(c.fst_dt)
               else null end as dt_dx_hypo_bl_6m
        , case when isnotnull(min(b.fst_dt)) then count(distinct b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then count(distinct c.fst_dt)
               else null end as n_dx_hypo_bl_6m
from ty33_pat_all_enrol_demog a left join ty33_dx_hypo b on a.patid=b.patid and b.fst_dt between date_sub(a.dt_rx_index,180) and date_sub(a.dt_rx_index,1) and b.hypo_type='Hypo'
                                left join ty33_dx_hypo c on a.patid=c.patid and c.fst_dt between date_sub(a.dt_rx_index,180) and date_sub(a.dt_rx_index,1) and c.hypo_type='Hypo_inc'
                                left join ty33_dx_hypo d on a.patid=d.patid and d.fst_dt between date_sub(a.dt_rx_index,180) and date_sub(a.dt_rx_index,1) and d.hypo_type='Hypo_exc'
group by a.patid, a.dt_rx_index
order by a.patid
;

select * from ty33_bl_6m_dx_hypo;



-- COMMAND ----------

drop table if exists ty33_fu_6m_a1c;

create table ty33_fu_6m_a1c as
select distinct patid, fst_dt as dt_last_a1c_fu_6m, value as fu_6m_a1c_last, rank
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210))
where rank<=1
order by patid
;

select * from ty33_fu_6m_a1c;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_fu_6m) as dt_a1c_start, max(dt_last_a1c_fu_6m) as dt_a1c_stop
from ty33_fu_6m_a1c
;

drop table if exists ty33_bl_6m_a1c;

create table ty33_bl_6m_a1c as
select distinct patid, fst_dt as dt_last_a1c_bl_6m, value as bl_6m_a1c_last, rank
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid
;

select * from ty33_bl_6m_a1c;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_bl_6m) as dt_a1c_start, max(dt_last_a1c_bl_6m) as dt_a1c_stop
from ty33_bl_6m_a1c
;



-- COMMAND ----------

drop table if exists ty33_fu_6m_med;

create table ty33_fu_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med;

select count(*) as n_obs, count(distinct patid) as n_pat
from ty33_fu_6m_med
;

drop table if exists ty33_fu_6m_med_dm;

create table ty33_fu_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_fu_6m_med a join (select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                            union
                            select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                            ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med_dm;

drop table if exists ty33_fu_6m_med_dm_max;

create table ty33_fu_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_fu_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_fu_6m_med_dm_max;

drop table if exists ty33_fu_6m_rx;

create table ty33_fu_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx;

drop table if exists ty33_fu_6m_rx_anti_dm;

create table ty33_fu_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_fu_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx_anti_dm;

drop table if exists ty33_pat_fu_6m_cost;

create table ty33_pat_fu_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.fu_6m_cost_med) as fu_6m_cost_med, max(b.fu_6m_cost_med_inp) as fu_6m_cost_med_inp, max(c.fu_6m_cost_med_er) as fu_6m_cost_med_er, max(d.fu_6m_cost_med_dm) as fu_6m_cost_med_dm
        , max(e.fu_6m_cost_med_dm_inp) as fu_6m_cost_med_dm_inp, max(f.fu_6m_cost_med_dm_er) as fu_6m_cost_med_dm_er, max(g.fu_6m_cost_rx) as fu_6m_cost_rx, max(h.fu_6m_cost_rx_anti_dm) as fu_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as fu_6m_cost_med from ty33_fu_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_inp from ty33_fu_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_er from ty33_fu_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm from ty33_fu_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_inp from ty33_fu_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_er from ty33_fu_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx from ty33_fu_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx_anti_dm from ty33_fu_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_cost
;

drop table if exists ty33_bl_6m_med;

create table ty33_bl_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med;

drop table if exists ty33_bl_6m_med_dm;

create table ty33_bl_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_bl_6m_med a join (select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med_dm;

drop table if exists ty33_bl_6m_med_dm_max;

create table ty33_bl_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_bl_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_bl_6m_med_dm_max;

drop table if exists ty33_bl_6m_rx;

create table ty33_bl_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx;

drop table if exists ty33_bl_6m_rx_anti_dm;

create table ty33_bl_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_bl_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx_anti_dm;

drop table if exists ty33_pat_bl_6m_cost;

create table ty33_pat_bl_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.bl_6m_cost_med) as bl_6m_cost_med, max(b.bl_6m_cost_med_inp) as bl_6m_cost_med_inp, max(c.bl_6m_cost_med_er) as bl_6m_cost_med_er, max(d.bl_6m_cost_med_dm) as bl_6m_cost_med_dm
        , max(e.bl_6m_cost_med_dm_inp) as bl_6m_cost_med_dm_inp, max(f.bl_6m_cost_med_dm_er) as bl_6m_cost_med_dm_er, max(g.bl_6m_cost_rx) as bl_6m_cost_rx, max(h.bl_6m_cost_rx_anti_dm) as bl_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as bl_6m_cost_med from ty33_bl_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_inp from ty33_bl_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_er from ty33_bl_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm from ty33_bl_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_inp from ty33_bl_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_er from ty33_bl_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx from ty33_bl_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx_anti_dm from ty33_bl_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_cost
;


-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_comorb1;

create table ty33_fu_6m_dx_comorb1 as
select distinct patid, dx_name, fst_dt as dt_last_dx_fu_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_11_16 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, dx_name
;

select * from ty33_fu_6m_dx_comorb1;

drop table if exists ty33_fu_6m_dx_comorb2;

create table ty33_fu_6m_dx_comorb2 as
select distinct patid, dx_name, fst_dt as dt_last_dx_fu_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_17_22 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, dx_name
;

select * from ty33_fu_6m_dx_comorb2;

drop table if exists ty33_fu_6m_dx_comorb;

create table ty33_fu_6m_dx_comorb as
select distinct coalesce(a.patid, b.patid) as patid, coalesce(a.dx_name, b.dx_name) as dx_name, coalesce(a.Disease, b.Disease) as Disease, coalesce(a.weight, b.weight) as weight
        , coalesce(a.weight_old, b.weight_old) as weight_old, greatest(a.dt_last_dx_fu_6m, b.dt_last_dx_fu_6m) as dt_last_dx_fu_6m
from ty33_fu_6m_dx_comorb1 a full join ty33_fu_6m_dx_comorb2 b
on a.patid=b.patid and a.dx_name=b.dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_fu_6m) as dt_dx_start, max(dt_last_dx_fu_6m) as dt_dx_stop
from ty33_fu_6m_dx_comorb
group by dx_name
order by dx_name
;

drop table if exists ty33_bl_6m_dx_comorb1;

create table ty33_bl_6m_dx_comorb1 as
select distinct patid, dx_name, fst_dt as dt_last_dx_bl_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_11_16 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, dx_name
;

select * from ty33_bl_6m_dx_comorb1;

drop table if exists ty33_bl_6m_dx_comorb2;

create table ty33_bl_6m_dx_comorb2 as
select distinct patid, dx_name, fst_dt as dt_last_dx_bl_6m, dt_rx_index, Disease, weight, weight_old, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.dx_name order by a.fst_dt desc, a.diag_position) as rank from ty19_dx_subset_17_22 a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, dx_name
;

select * from ty33_bl_6m_dx_comorb2;

drop table if exists ty33_bl_6m_dx_comorb;

create table ty33_bl_6m_dx_comorb as
select distinct coalesce(a.patid, b.patid) as patid, coalesce(a.dx_name, b.dx_name) as dx_name, coalesce(a.Disease, b.Disease) as Disease, coalesce(a.weight, b.weight) as weight
        , coalesce(a.weight_old, b.weight_old) as weight_old, greatest(a.dt_last_dx_bl_6m, b.dt_last_dx_bl_6m) as dt_last_dx_bl_6m
from ty33_bl_6m_dx_comorb1 a full join ty33_bl_6m_dx_comorb2 b
on a.patid=b.patid and a.dx_name=b.dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_bl_6m) as dt_dx_start, max(dt_last_dx_bl_6m) as dt_dx_stop
from ty33_bl_6m_dx_comorb
group by dx_name
order by dx_name
;

select dx_name, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_dx_bl_6m) as dt_dx_start, max(dt_last_dx_bl_6m) as dt_dx_stop
from ty33_bl_6m_dx_comorb
group by dx_name
order by dx_name
;



-- COMMAND ----------

drop table if exists ty33_fu_6m_inp_er;

create table ty33_fu_6m_inp_er as
select distinct a.patid, a.clmid, a.fst_dt, a.lst_dt, a.pos, a.proc_cd, a.conf_id, b.dt_rx_index
from (select * from ty19_ses_2208_med_claim where pos in ('21', '23')) a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_inp_er;

drop table if exists ty33_fu_6m_inp_er_dm;

create table ty33_fu_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_fu_6m_inp_er a join (select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_inp_er_dm;

drop table if exists ty33_pat_fu_6m_inp_er;

create table ty33_pat_fu_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.fu_6m_inp) as fu_6m_inp, max(e.fu_6m_inp_n) as fu_6m_inp_n, max(e.fu_6m_inp_days) as fu_6m_inp_days, max(b.fu_6m_er) as fu_6m_er, max(f.fu_6m_er_n) as fu_6m_er_n
        , max(c.fu_6m_inp_dm) as fu_6m_inp_dm, max(g.fu_6m_inp_n_dm) as fu_6m_inp_n_dm, max(g.fu_6m_inp_days_dm) as fu_6m_inp_days_dm
        , max(d.fu_6m_er_dm) as fu_6m_er_dm, max(h.fu_6m_er_n_dm) as fu_6m_er_n_dm
from (select distinct patid, 1 as fu_6m_inp from ty33_fu_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as fu_6m_er from ty33_fu_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as fu_6m_inp_dm from ty33_fu_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as fu_6m_er_dm from ty33_fu_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days from ty33_fu_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n from ty33_fu_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days_dm from ty33_fu_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n_dm from ty33_fu_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_inp_er
;

select a.*, b.n_obs
from ty33_pat_fu_6m_inp_er a join (select patid1, count(*) as n_obs from ty33_pat_fu_6m_inp_er group by patid1) b
on a.patid1=b.patid1
where b.n_obs>1
order by a.patid1
;

drop table if exists ty33_bl_6m_inp_er;

create table ty33_bl_6m_inp_er as
select distinct a.patid, a.clmid, a.fst_dt, a.lst_dt, a.pos, a.proc_cd, a.conf_id, b.dt_rx_index
from (select * from ty19_ses_2208_med_claim where pos in ('21', '23')) a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er;

drop table if exists ty33_bl_6m_inp_er_dm;

create table ty33_bl_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_bl_6m_inp_er a join (select * from ty19_dx_subset_11_16 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where diag_position in ('01','02') and dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er_dm;

drop table if exists ty33_pat_bl_6m_inp_er;

create table ty33_pat_bl_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.bl_6m_inp) as bl_6m_inp, max(e.bl_6m_inp_n) as bl_6m_inp_n, max(e.bl_6m_inp_days) as bl_6m_inp_days, max(b.bl_6m_er) as bl_6m_er, max(f.bl_6m_er_n) as bl_6m_er_n
        , max(c.bl_6m_inp_dm) as bl_6m_inp_dm, max(g.bl_6m_inp_n_dm) as bl_6m_inp_n_dm, max(g.bl_6m_inp_days_dm) as bl_6m_inp_days_dm
        , max(d.bl_6m_er_dm) as bl_6m_er_dm, max(h.bl_6m_er_n_dm) as bl_6m_er_n_dm
from (select distinct patid, 1 as bl_6m_inp from ty33_bl_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as bl_6m_er from ty33_bl_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as bl_6m_inp_dm from ty33_bl_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as bl_6m_er_dm from ty33_bl_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days from ty33_bl_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n from ty33_bl_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days_dm from ty33_bl_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n_dm from ty33_bl_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_inp_er
;

select a.*, b.n_obs
from ty33_pat_bl_6m_inp_er a join (select patid1, count(*) as n_obs from ty33_pat_bl_6m_inp_er group by patid1) b
on a.patid1=b.patid1
where b.n_obs>1
order by a.patid1
;


-- COMMAND ----------

drop table if exists ty33_fu_6m_rx;

create table ty33_fu_6m_rx as
select distinct patid, rx_type, fill_dt as dt_last_rx_fu_6m, dt_rx_index, gnrc_nm, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.rx_type order by a.fill_dt desc, a.ndc) as rank from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fill_dt between b.dt_rx_index and date_add(b.dt_rx_index,179))
where rank<=1
order by patid, rx_type
;

select * from ty33_fu_6m_rx;

select rx_type, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_rx_fu_6m) as dt_rx_start, max(dt_last_rx_fu_6m) as dt_rx_stop
from ty33_fu_6m_rx
group by rx_type
order by rx_type
;

drop table if exists ty33_bl_6m_rx;

create table ty33_bl_6m_rx as
select distinct patid, rx_type, fill_dt as dt_last_rx_bl_6m, dt_rx_index, gnrc_nm, rank
from (select a.*, b.dt_rx_index, dense_rank() over (partition by a.patid, a.rx_type order by a.fill_dt desc, a.ndc) as rank from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1))
where rank<=1
order by patid, rx_type
;

select * from ty33_bl_6m_rx;

select rx_type, format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_rx_bl_6m) as dt_rx_start, max(dt_last_rx_bl_6m) as dt_rx_stop
from ty33_bl_6m_rx
group by rx_type
order by rx_type
;



-- COMMAND ----------

drop table if exists ty33_index_social;

create table ty33_index_social as
select distinct a.patid, d_education_level_code,d_fed_poverty_status_code,d_home_ownership_code,d_household_income_range_code,d_networth_range_code,d_occupation_type_code,d_race_code,num_adults,num_child, dt_rx_index
from ty19_ses_2208_Socioeconomic a join ty33_pat_all_enrol_demog b
on a.patid=b.patid
order by a.patid
;

select * from ty33_index_social;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_rx_index) as dt_start, max(dt_rx_index) as dt_stop
from ty33_index_social
;



-- COMMAND ----------

drop table if exists ty33_bl_6m_rx_glp1;

create table ty33_bl_6m_rx_glp1 as
select distinct a.patid, a.brnd_nm, a.fill_dt, b.dt_rx_index
from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1) and lcase(brnd_nm) in ('trulicity','ozempic','bydureon','bydureon bcise','bydureon pen')
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx_glp1;

-- COMMAND ----------

drop table if exists ty33_bl_6m_rx_glp1_pat;

create table ty33_bl_6m_rx_glp1_pat as
select distinct coalesce(a.patid,b.patid,c.patid) as patid1, max(a.fl_trulicity) as bl_6m_rx_trulicity, max(b.fl_ozempic) as bl_6m_rx_ozempic, max(c.fl_bydureon) as bl_6m_rx_bydureon
from (select patid, 1 as fl_trulicity from ty33_bl_6m_rx_glp1 where lcase(brnd_nm) in ('trulicity')) a
full join (select patid, 1 as fl_ozempic from ty33_bl_6m_rx_glp1 where lcase(brnd_nm) in ('ozempic')) b on a.patid=b.patid
full join (select patid, 1 as fl_bydureon from ty33_bl_6m_rx_glp1 where lcase(brnd_nm) in ('bydureon','bydureon bcise','bydureon pen')) c on a.patid=c.patid
group by patid1
;

select * from ty33_bl_6m_rx_glp1_pat;


-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct patid1),0) as n_pat
from ty33_bl_6m_rx_glp1_pat
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_pat_all_enrol_demog")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_all_enrol_demog")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_a1c")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_a1c")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_dx_comorb")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_dx_comorb")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_dx_comorb")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_dx_comorb")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_inp_er")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_inp_er")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_inp_er")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_inp_er")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_rx")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_rx")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_rx")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_rx")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_index_social")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_index_social")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_dx_hypo")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_dx_hypo")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_dx_hypo")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_dx_hypo")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_lab_glucose")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_lab_glucose")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_lab_glucose")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_lab_glucose")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_rx_glp1_pat")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_rx_glp1_pat")
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

drop table if exists ty33_bl_6m_a1c;

create table ty33_bl_6m_a1c as
select distinct a1.patid, a1.fst_dt as dt_last_a1c_bl_6m, a1.value as bl_6m_a1c_last, a1.rank, b1.bl_6m_n_a1c
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
) a1 left join
(select a.patid, count(distinct fst_dt) as bl_6m_n_a1c from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
group by a.patid) b1 on a1.patid=b1.patid
where a1.rank<=1
order by a1.patid
;

select * from ty33_bl_6m_a1c;

-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_bl_6m) as dt_a1c_start, max(dt_last_a1c_bl_6m) as dt_a1c_stop
from ty33_bl_6m_a1c
;

-- COMMAND ----------

drop table if exists ty33_fu_6m_a1c;

create table ty33_fu_6m_a1c as
select distinct a1.patid, a1.fst_dt as dt_last_a1c_fu_6m, a1.value as fu_6m_a1c_last, a1.rank, b1.fu_6m_n_a1c
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.value desc) as rank from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210)
) a1 left join
(select a.patid, count(distinct fst_dt) as fu_6m_n_a1c from ty33_lab_a1c_index a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210)
group by a.patid) b1 on a1.patid=b1.patid
where a1.rank<=1
order by a1.patid
;

select * from ty33_fu_6m_a1c;

-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_a1c_fu_6m) as dt_a1c_start, max(dt_last_a1c_fu_6m) as dt_a1c_stop
from ty33_fu_6m_a1c
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_a1c")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_a1c")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_a1c")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

select tst_desc, count(*) as n_obs
from ty19_ses_2208_lab_result
where lcase(tst_desc) like '%glucose%'
group by tst_desc
order by n_obs desc
;


-- COMMAND ----------

select * from ty19_ses_2208_lab_result;


-- COMMAND ----------

drop table if exists ty33_fu_6m_glucose;

create table ty33_fu_6m_glucose as
select distinct a1.patid, a1.fst_dt as dt_last_glucose_fu_6m, a1.rslt_nbr as fu_6m_glucose_last, a1.rank, b1.fu_6m_n_glucose
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.rslt_nbr desc) as rank from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210)
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
) a1 left join
(select a.patid, count(distinct fst_dt) as fu_6m_n_glucose from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210)
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
group by a.patid) b1 on a1.patid=b1.patid
where a1.rank<=1
order by a1.patid
;

select * from ty33_fu_6m_glucose;


-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_glucose_fu_6m) as dt_glucose_start, max(dt_last_glucose_fu_6m) as dt_glucose_stop
from ty33_fu_6m_glucose
;

-- COMMAND ----------

drop table if exists ty33_bl_6m_glucose;

create table ty33_bl_6m_glucose as
select distinct a1.patid, a1.fst_dt as dt_last_glucose_bl_6m, a1.rslt_nbr as bl_6m_glucose_last, a1.rank, b1.bl_6m_n_glucose
from (select a.*, dense_rank() over (partition by a.patid order by a.fst_dt desc, a.rslt_nbr desc) as rank from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
) a1 left join
(select a.patid, count(distinct fst_dt) as bl_6m_n_glucose from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
group by a.patid) b1 on a1.patid=b1.patid
where a1.rank<=1
order by a1.patid
;

select * from ty33_bl_6m_glucose;


-- COMMAND ----------

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_glucose_bl_6m) as dt_glucose_start, max(dt_last_glucose_bl_6m) as dt_glucose_stop
from ty33_bl_6m_glucose
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_glucose")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_glucose")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_glucose")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_glucose")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty33_fu_6m_dx_hypo;

create table ty33_fu_6m_dx_hypo as
select distinct a.patid, a.dt_rx_index, min(b.fst_dt) as dt_dx_hypo, min(c.fst_dt) as dt_dx_hypo_inc, min(d.fst_dt) as dt_dx_hypo_exc
        , case when isnotnull(min(b.fst_dt)) then min(b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then min(c.fst_dt)
               else null end as dt_start_dx_hypo_fu_6m
        , case when isnotnull(max(b.fst_dt)) then max(b.fst_dt)
               when isnotnull(max(c.fst_dt)) and isnull(max(d.fst_dt)) then max(c.fst_dt)
               else null end as dt_dx_end_hypo_fu_6m
        , case when isnotnull(min(b.fst_dt)) then count(distinct b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then count(distinct c.fst_dt)
               else null end as n_dx_hypo_fu_6m
from ty33_pat_all_enrol_demog a left join ty33_dx_hypo b on a.patid=b.patid and b.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and b.hypo_type='Hypo'
                                left join ty33_dx_hypo c on a.patid=c.patid and c.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and c.hypo_type='Hypo_inc'
                                left join ty33_dx_hypo d on a.patid=d.patid and d.fst_dt between a.dt_rx_index and date_add(a.dt_rx_index,179) and d.hypo_type='Hypo_exc'
group by a.patid, a.dt_rx_index
order by a.patid
;

select * from ty33_fu_6m_dx_hypo;

drop table if exists ty33_bl_6m_dx_hypo;

create table ty33_bl_6m_dx_hypo as
select distinct a.patid, a.dt_rx_index, min(b.fst_dt) as dt_dx_hypo, min(c.fst_dt) as dt_dx_hypo_inc, min(d.fst_dt) as dt_dx_hypo_exc
        , case when isnotnull(min(b.fst_dt)) then min(b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then min(c.fst_dt)
               else null end as dt_start_dx_hypo_bl_6m
        , case when isnotnull(max(b.fst_dt)) then max(b.fst_dt)
               when isnotnull(max(c.fst_dt)) and isnull(max(d.fst_dt)) then max(c.fst_dt)
               else null end as dt_dx_end_hypo_bl_6m
        , case when isnotnull(min(b.fst_dt)) then count(distinct b.fst_dt)
               when isnotnull(min(c.fst_dt)) and isnull(min(d.fst_dt)) then count(distinct c.fst_dt)
               else null end as n_dx_hypo_bl_6m
from ty33_pat_all_enrol_demog a left join ty33_dx_hypo b on a.patid=b.patid and b.fst_dt between date_sub(a.dt_rx_index,180) and date_sub(a.dt_rx_index,1) and b.hypo_type='Hypo'
                                left join ty33_dx_hypo c on a.patid=c.patid and c.fst_dt between date_sub(a.dt_rx_index,180) and date_sub(a.dt_rx_index,1) and c.hypo_type='Hypo_inc'
                                left join ty33_dx_hypo d on a.patid=d.patid and d.fst_dt between date_sub(a.dt_rx_index,180) and date_sub(a.dt_rx_index,1) and d.hypo_type='Hypo_exc'
group by a.patid, a.dt_rx_index
order by a.patid
;

select * from ty33_bl_6m_dx_hypo;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_dx_hypo")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_dx_hypo")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_dx_hypo")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_dx_hypo")
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

drop table if exists ty33_fu_6m_inp_er_dm;

create table ty33_fu_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_fu_6m_inp_er a join (select * from ty19_dx_subset_11_16 where dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_inp_er_dm;

drop table if exists ty33_pat_fu_6m_inp_er;

create table ty33_pat_fu_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.fu_6m_inp) as fu_6m_inp, max(e.fu_6m_inp_n) as fu_6m_inp_n, max(e.fu_6m_inp_days) as fu_6m_inp_days, max(b.fu_6m_er) as fu_6m_er, max(f.fu_6m_er_n) as fu_6m_er_n
        , max(c.fu_6m_inp_dm) as fu_6m_inp_dm, max(g.fu_6m_inp_n_dm) as fu_6m_inp_n_dm, max(g.fu_6m_inp_days_dm) as fu_6m_inp_days_dm
        , max(d.fu_6m_er_dm) as fu_6m_er_dm, max(h.fu_6m_er_n_dm) as fu_6m_er_n_dm
from (select distinct patid, 1 as fu_6m_inp from ty33_fu_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as fu_6m_er from ty33_fu_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as fu_6m_inp_dm from ty33_fu_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as fu_6m_er_dm from ty33_fu_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days from ty33_fu_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n from ty33_fu_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days_dm from ty33_fu_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n_dm from ty33_fu_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_inp_er
;

select a.*, b.n_obs
from ty33_pat_fu_6m_inp_er a join (select patid1, count(*) as n_obs from ty33_pat_fu_6m_inp_er group by patid1) b
on a.patid1=b.patid1
where b.n_obs>1
order by a.patid1
;

drop table if exists ty33_bl_6m_inp_er;

create table ty33_bl_6m_inp_er as
select distinct a.patid, a.clmid, a.fst_dt, a.lst_dt, a.pos, a.proc_cd, a.conf_id, b.dt_rx_index
from (select * from ty19_ses_2208_med_claim where pos in ('21', '23')) a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er;

drop table if exists ty33_bl_6m_inp_er_dm;

create table ty33_bl_6m_inp_er_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name, b.fst_dt as dt_dx
from ty33_bl_6m_inp_er a join (select * from ty19_dx_subset_11_16 where dx_name in ('T2DM', 'T1DM')
                               union
                               select * from ty19_dx_subset_17_22 where dx_name in ('T2DM', 'T1DM')
                                ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_inp_er_dm;

drop table if exists ty33_pat_bl_6m_inp_er;

create table ty33_pat_bl_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.bl_6m_inp) as bl_6m_inp, max(e.bl_6m_inp_n) as bl_6m_inp_n, max(e.bl_6m_inp_days) as bl_6m_inp_days, max(b.bl_6m_er) as bl_6m_er, max(f.bl_6m_er_n) as bl_6m_er_n
        , max(c.bl_6m_inp_dm) as bl_6m_inp_dm, max(g.bl_6m_inp_n_dm) as bl_6m_inp_n_dm, max(g.bl_6m_inp_days_dm) as bl_6m_inp_days_dm
        , max(d.bl_6m_er_dm) as bl_6m_er_dm, max(h.bl_6m_er_n_dm) as bl_6m_er_n_dm
from (select distinct patid, 1 as bl_6m_inp from ty33_bl_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as bl_6m_er from ty33_bl_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as bl_6m_inp_dm from ty33_bl_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as bl_6m_er_dm from ty33_bl_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days from ty33_bl_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n from ty33_bl_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as bl_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as bl_6m_inp_days_dm from ty33_bl_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as bl_6m_er_n_dm from ty33_bl_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_inp_er
;

select a.*, b.n_obs
from ty33_pat_bl_6m_inp_er a join (select patid1, count(*) as n_obs from ty33_pat_bl_6m_inp_er group by patid1) b
on a.patid1=b.patid1
where b.n_obs>1
order by a.patid1
;


-- COMMAND ----------

drop table if exists ty33_pat_fu_6m_inp_er;

create table ty33_pat_fu_6m_inp_er as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid,e.patid,f.patid,g.patid,h.patid) as patid1
        , max(a.fu_6m_inp) as fu_6m_inp, max(e.fu_6m_inp_n) as fu_6m_inp_n, max(e.fu_6m_inp_days) as fu_6m_inp_days, min(e.dt_1st_fu_6m_inp) as dt_1st_fu_6m_inp
        , max(b.fu_6m_er) as fu_6m_er, max(f.fu_6m_er_n) as fu_6m_er_n, min(f.dt_1st_fu_6m_er) as dt_1st_fu_6m_er
        , max(c.fu_6m_inp_dm) as fu_6m_inp_dm, max(g.fu_6m_inp_n_dm) as fu_6m_inp_n_dm, max(g.fu_6m_inp_days_dm) as fu_6m_inp_days_dm, min(g.dt_1st_fu_6m_inp_dm) as dt_1st_fu_6m_inp_dm
        , max(d.fu_6m_er_dm) as fu_6m_er_dm, max(h.fu_6m_er_n_dm) as fu_6m_er_n_dm, min(h.dt_1st_fu_6m_er_dm) as dt_1st_fu_6m_er_dm
from (select distinct patid, 1 as fu_6m_inp from ty33_fu_6m_inp_er where pos='21') a
        full join (select distinct patid, 1 as fu_6m_er from ty33_fu_6m_inp_er where pos='23') b on a.patid=b.patid
        full join (select distinct patid, 1 as fu_6m_inp_dm from ty33_fu_6m_inp_er_dm where pos='21') c on a.patid=c.patid
        full join (select distinct patid, 1 as fu_6m_er_dm from ty33_fu_6m_inp_er_dm where pos='23') d on a.patid=d.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days, min(fst_dt) as dt_1st_fu_6m_inp from ty33_fu_6m_inp_er where pos='21' and isnotnull(conf_id) group by patid) e on a.patid=e.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n, min(fst_dt) as dt_1st_fu_6m_er from ty33_fu_6m_inp_er where pos='23' and isnotnull(fst_dt) group by patid) f on a.patid=f.patid
        full join (select distinct patid, count(distinct conf_id) as fu_6m_inp_n_dm, ceil(mean(datediff(lst_dt,fst_dt)+1)) as fu_6m_inp_days_dm, min(fst_dt) as dt_1st_fu_6m_inp_dm from ty33_fu_6m_inp_er_dm where pos='21' and isnotnull(conf_id) group by patid) g on a.patid=g.patid
        full join (select distinct patid, count(distinct fst_dt) as fu_6m_er_n_dm, min(fst_dt) as dt_1st_fu_6m_er_dm from ty33_fu_6m_inp_er_dm where pos='23' and isnotnull(dt_dx) group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select * from ty33_pat_fu_6m_inp_er;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_inp_er")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_inp_er")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_inp_er")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_inp_er")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty33_fu_6m_med_dm;

create table ty33_fu_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_fu_6m_med a join (select * from ty19_dx_subset_17_22 where dx_name in ('T2DM', 'T1DM')
                            union
                            select * from ty19_dx_subset_11_16 where dx_name in ('T2DM', 'T1DM')
                            ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med_dm;

drop table if exists ty33_fu_6m_med_dm_max;

create table ty33_fu_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_fu_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_fu_6m_med_dm_max;

drop table if exists ty33_fu_6m_rx;

create table ty33_fu_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between b.dt_rx_index and date_add(b.dt_rx_index,179)
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx;

drop table if exists ty33_fu_6m_rx_anti_dm;

create table ty33_fu_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_fu_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_fu_6m_rx_anti_dm;

drop table if exists ty33_pat_fu_6m_cost;

create table ty33_pat_fu_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.fu_6m_cost_med) as fu_6m_cost_med, max(b.fu_6m_cost_med_inp) as fu_6m_cost_med_inp, max(c.fu_6m_cost_med_er) as fu_6m_cost_med_er, max(d.fu_6m_cost_med_dm) as fu_6m_cost_med_dm
        , max(e.fu_6m_cost_med_dm_inp) as fu_6m_cost_med_dm_inp, max(f.fu_6m_cost_med_dm_er) as fu_6m_cost_med_dm_er, max(g.fu_6m_cost_rx) as fu_6m_cost_rx, max(h.fu_6m_cost_rx_anti_dm) as fu_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as fu_6m_cost_med from ty33_fu_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_inp from ty33_fu_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_er from ty33_fu_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm from ty33_fu_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_inp from ty33_fu_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_er from ty33_fu_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx from ty33_fu_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx_anti_dm from ty33_fu_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_cost
;

drop table if exists ty33_bl_6m_med;

create table ty33_bl_6m_med as
select distinct a.patid, a.clmid, a.fst_dt, a.pos, a.proc_cd, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_med_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med;

drop table if exists ty33_bl_6m_med_dm;

create table ty33_bl_6m_med_dm as
select distinct a.*, b.diag, b.diag_position, b.dx_name
from ty33_bl_6m_med a join (select * from ty19_dx_subset_17_22 where dx_name in ('T2DM', 'T1DM')) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med_dm;

drop table if exists ty33_bl_6m_med_dm_max;

create table ty33_bl_6m_med_dm_max as
select *
from (select *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, diag_position) as rank from ty33_bl_6m_med_dm)
where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ty33_bl_6m_med_dm_max;

drop table if exists ty33_bl_6m_rx;

create table ty33_bl_6m_rx as
select distinct a.patid, a.clmid, a.fill_dt, a.brnd_nm, a.ndc, a.gnrc_nm, a.days_sup, a.quantity, a.std_cost, a.std_cost_yr, b.dt_rx_index
from ty19_ses_2208_rx_claim a join (select distinct patid, dt_rx_index from ty33_pat_all_enrol_demog) b
on a.patid=b.patid and a.fill_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1)
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx;

drop table if exists ty33_bl_6m_rx_anti_dm;

create table ty33_bl_6m_rx_anti_dm as
select distinct a.*, b.rx_type
from ty33_bl_6m_rx a join ty00_ses_rx_anti_dm_loopup b
on a.ndc=b.ndc
order by a.patid, a.fill_dt
;

select * from ty33_bl_6m_rx_anti_dm;

drop table if exists ty33_pat_bl_6m_cost;

create table ty33_pat_bl_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.bl_6m_cost_med) as bl_6m_cost_med, max(b.bl_6m_cost_med_inp) as bl_6m_cost_med_inp, max(c.bl_6m_cost_med_er) as bl_6m_cost_med_er, max(d.bl_6m_cost_med_dm) as bl_6m_cost_med_dm
        , max(e.bl_6m_cost_med_dm_inp) as bl_6m_cost_med_dm_inp, max(f.bl_6m_cost_med_dm_er) as bl_6m_cost_med_dm_er, max(g.bl_6m_cost_rx) as bl_6m_cost_rx, max(h.bl_6m_cost_rx_anti_dm) as bl_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as bl_6m_cost_med from ty33_bl_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_inp from ty33_bl_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_er from ty33_bl_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm from ty33_bl_6m_med_dm_max group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_inp from ty33_bl_6m_med_dm_max where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_er from ty33_bl_6m_med_dm_max where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx from ty33_bl_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx_anti_dm from ty33_bl_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_cost
;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty33_fu_6m_hypo_glucose;

create table ty33_fu_6m_hypo_glucose as
select distinct a.patid, min(a.fst_dt) as dt_1st_hypo_glucose_fu_6m, max(a.fst_dt) as dt_last_hypo_glucose_fu_6m, count(distinct a.fst_dt) as n_hypo_glucose_fu_6m, min(a.rslt_nbr) as fu_6m_hypo_glucose_mean
from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,90) and date_add(b.dt_rx_index,210) and isnotnull(a.rslt_nbr) and a.rslt_nbr<70
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
group by a.patid
order by a.patid
;

select * from ty33_fu_6m_hypo_glucose;


-- COMMAND ----------

drop table if exists ty33_bl_6m_hypo_glucose;

create table ty33_bl_6m_hypo_glucose as
select distinct a.patid, min(a.fst_dt) as dt_1st_hypo_glucose_bl_6m, max(a.fst_dt) as dt_last_hypo_glucose_bl_6m, count(distinct a.fst_dt) as n_hypo_glucose_bl_6m, min(a.rslt_nbr) as bl_6m_hypo_glucose_mean
from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1) and isnotnull(a.rslt_nbr) and a.rslt_nbr<70
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
group by a.patid
order by a.patid
;

select * from ty33_bl_6m_hypo_glucose;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_hypo_glucose")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_hypo_glucose")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_fu_6m_hypo_glucose")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_fu_6m_hypo_glucose")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty33_bl_6m_rx_glp1_pat;

create table ty33_bl_6m_rx_glp1_pat as
select distinct coalesce(a.patid,b.patid,c.patid) as patid1, max(a.fl_trulicity) as bl_6m_rx_trulicity, max(b.fl_ozempic) as bl_6m_rx_ozempic, max(c.fl_bydureon) as bl_6m_rx_bydureon
, min(a.fill_dt) as dt_1st_rx_trulicity_bl_6m, min(b.fill_dt) as dt_1st_rx_ozempic_bl_6m, min(c.fill_dt) as dt_1st_rx_bydureon_bl_6m, min(least(a.fill_dt,b.fill_dt,c.fill_dt))as dt_1st_rx_glp1_bl_6m
from (select patid, 1 as fl_trulicity, fill_dt from ty33_bl_6m_rx_glp1 where lcase(brnd_nm) in ('trulicity')) a
full join (select patid, 1 as fl_ozempic, fill_dt from ty33_bl_6m_rx_glp1 where lcase(brnd_nm) in ('ozempic')) b on a.patid=b.patid
full join (select patid, 1 as fl_bydureon, fill_dt from ty33_bl_6m_rx_glp1 where lcase(brnd_nm) in ('bydureon','bydureon bcise','bydureon pen')) c on a.patid=c.patid
group by patid1
;

select * from ty33_bl_6m_rx_glp1_pat;

-- COMMAND ----------

drop table if exists ty33_bl_18m_rx_glp1_pat;

create table ty33_bl_18m_rx_glp1_pat as
select distinct coalesce(a.patid,b.patid,c.patid) as patid1, max(a.fl_trulicity) as bl_18m_rx_trulicity, max(b.fl_ozempic) as bl_18m_rx_ozempic, max(c.fl_bydureon) as bl_18m_rx_bydureon
, min(a.fill_dt) as dt_1st_rx_trulicity_bl_18m, min(b.fill_dt) as dt_1st_rx_ozempic_bl_18m, min(c.fill_dt) as dt_1st_rx_bydureon_bl_18m, min(least(a.fill_dt,b.fill_dt,c.fill_dt)) as dt_1st_rx_glp1_bl_18m
from (select patid, 1 as fl_trulicity, fill_dt from ty33_bl_18m_rx_glp1 where lcase(brnd_nm) in ('trulicity')) a
full join (select patid, 1 as fl_ozempic, fill_dt from ty33_bl_18m_rx_glp1 where lcase(brnd_nm) in ('ozempic')) b on a.patid=b.patid
full join (select patid, 1 as fl_bydureon, fill_dt from ty33_bl_18m_rx_glp1 where lcase(brnd_nm) in ('bydureon','bydureon bcise','bydureon pen')) c on a.patid=c.patid
group by patid1
;

select * from ty33_bl_18m_rx_glp1_pat;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_bl_6m_rx_glp1_pat")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_6m_rx_glp1_pat")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_bl_18m_rx_glp1_pat")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_bl_18m_rx_glp1_pat")
-- MAGIC
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

drop table if exists ty33_fu_6m_hypo_glucose;

create table ty33_fu_6m_hypo_glucose as
select distinct a.patid, min(a.fst_dt) as dt_1st_hypo_glucose_fu_6m, max(a.fst_dt) as dt_last_hypo_glucose_fu_6m, count(distinct a.fst_dt) as n_hypo_glucose_fu_6m, mean(a.rslt_nbr) as fu_6m_hypo_glucose_mean
from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_add(b.dt_rx_index,0) and date_add(b.dt_rx_index,179) and isnotnull(a.rslt_nbr) and a.rslt_nbr<70
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
group by a.patid
order by a.patid
;

select * from ty33_fu_6m_hypo_glucose;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_hypo_glucose_fu_6m) as dt_hypo_glucose_start, max(dt_last_hypo_glucose_fu_6m) as dt_hypo_glucose_stop
from ty33_fu_6m_hypo_glucose
;

drop table if exists ty33_bl_6m_hypo_glucose;

create table ty33_bl_6m_hypo_glucose as
select distinct a.patid, min(a.fst_dt) as dt_1st_hypo_glucose_bl_6m, max(a.fst_dt) as dt_last_hypo_glucose_bl_6m, count(distinct a.fst_dt) as n_hypo_glucose_bl_6m, mean(a.rslt_nbr) as bl_6m_hypo_glucose_mean
from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_sub(b.dt_rx_index,1) and isnotnull(a.rslt_nbr) and a.rslt_nbr<70
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
group by a.patid
order by a.patid
;

select * from ty33_bl_6m_hypo_glucose;

select format_number(count(*),0) as n_obs, format_number(count(distinct patid),0) as n_pat, min(dt_last_hypo_glucose_bl_6m) as dt_hypo_glucose_start, max(dt_last_hypo_glucose_bl_6m) as dt_hypo_glucose_stop
from ty33_bl_6m_hypo_glucose
;



-- COMMAND ----------

drop table if exists ty33_hypo_lb;

create table ty33_hypo_lb as
select distinct a.patid, b.dt_rx_index, a.fst_dt, a.rslt_nbr, 'LB' as source
from ty19_ses_2208_lab_result a join ty33_pat_all_enrol_demog b
on a.patid=b.patid and a.fst_dt between date_sub(b.dt_rx_index,180) and date_add(b.dt_rx_index,179) and isnotnull(a.rslt_nbr) and a.rslt_nbr<70
and lcase(a.tst_desc) in ('glucose','glucose, serum','glucose,fasting','mean plasma glucose','glucose, plasma','glucose mean value','estimated average glucose','glucose, fasting'
,'glucose,plasma','glucose - fasting','glucose, fasting (p)','est mean whole bld glucose','mean blood glucose','calculated mean glucose','estimated average glucose') and a.rslt_nbr>0
order by a.patid
;

select * from ty33_hypo_lb;

-- COMMAND ----------

drop table if exists ty33_hypo_dx;

create table ty33_hypo_dx as
select distinct a.patid, a.dt_rx_index, 'DX' as source, b.fst_dt as dt_dx_hypo, c.fst_dt as dt_dx_hypo_inc, d.fst_dt as dt_dx_hypo_exc
, case when isnotnull(b.fst_dt) then b.fst_dt
       when isnotnull(c.fst_dt) and isnull(d.fst_dt) then c.fst_dt
       else null end as dt_hypo
from ty33_pat_all_enrol_demog a left join ty33_dx_hypo b on a.patid=b.patid and b.hypo_type='Hypo'
                                left join ty33_dx_hypo c on a.patid=c.patid and c.hypo_type='Hypo_inc'
                                left join ty33_dx_hypo d on a.patid=d.patid and d.hypo_type='Hypo_exc'
order by a.patid
;

select * from ty33_hypo_dx;

-- COMMAND ----------

drop table if exists ty33_hypo_dx_lb;

create table ty33_hypo_dx_lb as
select patid, dt_rx_index, dt_hypo, source from ty33_hypo_dx where isnotnull(dt_hypo)
union
select patid, dt_rx_index, fst_dt as dt_hypo, source from ty33_hypo_lb
order by patid, dt_hypo
;

select * from ty33_hypo_dx_lb;

-- COMMAND ----------

drop table if exists ty33_hypo_dx_lb_fu_6m;

create table ty33_hypo_dx_lb_fu_6m as
select distinct patid, min(dt_hypo) as dt_1st_hypo_dx_lb, max(dt_hypo) as dt_last_hypo_dx_lb, count(distinct dt_hypo) as n_hypo_dx_lb_fu_6m
from ty33_hypo_dx_lb
where dt_hypo between dt_rx_index and date_add(dt_rx_index,179)
group by patid
order by patid
;

select * from ty33_hypo_dx_lb_fu_6m;

-- COMMAND ----------

drop table if exists ty33_hypo_dx_lb_bl_6m;

create table ty33_hypo_dx_lb_bl_6m as
select distinct patid, min(dt_hypo) as dt_1st_hypo_dx_lb_bl_6m, max(dt_hypo) as dt_last_hypo_dx_lb_bl_6m, count(distinct dt_hypo) as n_hypo_dx_lb_bl_6m
from ty33_hypo_dx_lb
where dt_hypo between date_sub(dt_rx_index,180) and date_sub(dt_rx_index,1)
group by patid
order by patid
;

select * from ty33_hypo_dx_lb_bl_6m;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_hypo_dx_lb_bl_6m")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_hypo_dx_lb_bl_6m")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_hypo_dx_lb_fu_6m")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_hypo_dx_lb_fu_6m")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

select * from ty33_pat_all_enrol_demog;

-- COMMAND ----------

drop table if exists ty33_rx_records;

create table ty33_rx_records as
select distinct patid,pat_planid,clmid,copay,days_sup,deduct,fill_dt,quantity,specclss,std_cost,std_cost_yr,strength,ahfsclss_desc,rx_type,brnd_nm,gnrc_nm,ndc,dt_rx_index,index_group,index_group2,dt_rx_index2
       , case when lcase(brnd_nm) like '%toujeo solostar%' then 'Toujeo'
              when lcase(gnrc_nm) like '%insulin glargine,hum.rec.anlog%' and lcase(brnd_nm) not like '%toujeo%' then 'Gla-100'
              when lcase(gnrc_nm) like '%detemir%' then 'Detemir'
              when lcase(gnrc_nm) like '%nph%' then 'NPH'
              when lcase(rx_type) like '%glp1%' then 'GLP1'
                   else rx_type end as rx_type2
from (select a.*, b.dt_rx_index, b.index_group, b.index_group2, b.dt_rx_index2 from ty19_rx_anti_dm a join ty33_pat_all_enrol_demog b
on a.patid=b.patid)
order by patid, rx_type
;

select * from ty33_rx_records;

select rx_type2, count(distinct patid) as n_pat
from ty33_rx_records
group by rx_type2
order by rx_type2
;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_rx_records where rx_type2 in ('Toujeo','Gla-100','Detemir','NPH')")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_rx_records")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty33_pat_fu_6m_cost;

create table ty33_pat_fu_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.fu_6m_cost_med) as fu_6m_cost_med, max(b.fu_6m_cost_med_inp) as fu_6m_cost_med_inp, max(c.fu_6m_cost_med_er) as fu_6m_cost_med_er, max(d.fu_6m_cost_med_dm) as fu_6m_cost_med_dm
        , max(e.fu_6m_cost_med_dm_inp) as fu_6m_cost_med_dm_inp, max(f.fu_6m_cost_med_dm_er) as fu_6m_cost_med_dm_er, max(g.fu_6m_cost_rx) as fu_6m_cost_rx, max(h.fu_6m_cost_rx_anti_dm) as fu_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as fu_6m_cost_med from ty33_fu_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_inp from ty33_fu_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_er from ty33_fu_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm from ty33_fu_6m_med_dm group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_inp from ty33_fu_6m_med_dm where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_er from ty33_fu_6m_med_dm where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx from ty33_fu_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx_anti_dm from ty33_fu_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat

-- COMMAND ----------

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_cost
;

-- COMMAND ----------

drop table if exists ty33_pat_bl_6m_cost;

create table ty33_pat_bl_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.bl_6m_cost_med) as bl_6m_cost_med, max(b.bl_6m_cost_med_inp) as bl_6m_cost_med_inp, max(c.bl_6m_cost_med_er) as bl_6m_cost_med_er, max(d.bl_6m_cost_med_dm) as bl_6m_cost_med_dm
        , max(e.bl_6m_cost_med_dm_inp) as bl_6m_cost_med_dm_inp, max(f.bl_6m_cost_med_dm_er) as bl_6m_cost_med_dm_er, max(g.bl_6m_cost_rx) as bl_6m_cost_rx, max(h.bl_6m_cost_rx_anti_dm) as bl_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as bl_6m_cost_med from ty33_bl_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_inp from ty33_bl_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_er from ty33_bl_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm from ty33_bl_6m_med_dm group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_inp from ty33_bl_6m_med_dm where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_er from ty33_bl_6m_med_dm where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx from ty33_bl_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx_anti_dm from ty33_bl_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_cost
;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

drop table if exists ty33_fu_6m_med_dm;

create table ty33_fu_6m_med_dm as
select distinct a.*
from ty33_fu_6m_med a join (select * from ty19_dx_subset_17_22 where dx_name in ('T2DM', 'T1DM')
                            union
                            select * from ty19_dx_subset_11_16 where dx_name in ('T2DM', 'T1DM')
                            ) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_fu_6m_med_dm;


-- COMMAND ----------

drop table if exists ty33_pat_fu_6m_cost;

create table ty33_pat_fu_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.fu_6m_cost_med) as fu_6m_cost_med, max(b.fu_6m_cost_med_inp) as fu_6m_cost_med_inp, max(c.fu_6m_cost_med_er) as fu_6m_cost_med_er, max(d.fu_6m_cost_med_dm) as fu_6m_cost_med_dm
        , max(e.fu_6m_cost_med_dm_inp) as fu_6m_cost_med_dm_inp, max(f.fu_6m_cost_med_dm_er) as fu_6m_cost_med_dm_er, max(g.fu_6m_cost_rx) as fu_6m_cost_rx, max(h.fu_6m_cost_rx_anti_dm) as fu_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as fu_6m_cost_med from ty33_fu_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_inp from ty33_fu_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_er from ty33_fu_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm from ty33_fu_6m_med_dm group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_inp from ty33_fu_6m_med_dm where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_med_dm_er from ty33_fu_6m_med_dm where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx from ty33_fu_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as fu_6m_cost_rx_anti_dm from ty33_fu_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_fu_6m_cost
;


-- COMMAND ----------

select *
from ty33_pat_fu_6m_cost
where fu_6m_cost_med_dm_inp>fu_6m_cost_med_inp and isnotnull(fu_6m_cost_med_inp)
;

-- COMMAND ----------

drop table if exists ty33_bl_6m_med_dm;

create table ty33_bl_6m_med_dm as
select distinct a.*
from ty33_bl_6m_med a join (select * from ty19_dx_subset_17_22 where dx_name in ('T2DM', 'T1DM')) b
on a.patid=b.patid and a.clmid=b.clmid
order by a.patid, a.fst_dt
;

select * from ty33_bl_6m_med_dm;

-- COMMAND ----------

drop table if exists ty33_pat_bl_6m_cost;

create table ty33_pat_bl_6m_cost as
select distinct coalesce(a.patid, b.patid, c.patid, d.patid, e.patid, f.patid, g.patid, h.patid) as patid1
        , max(a.bl_6m_cost_med) as bl_6m_cost_med, max(b.bl_6m_cost_med_inp) as bl_6m_cost_med_inp, max(c.bl_6m_cost_med_er) as bl_6m_cost_med_er, max(d.bl_6m_cost_med_dm) as bl_6m_cost_med_dm
        , max(e.bl_6m_cost_med_dm_inp) as bl_6m_cost_med_dm_inp, max(f.bl_6m_cost_med_dm_er) as bl_6m_cost_med_dm_er, max(g.bl_6m_cost_rx) as bl_6m_cost_rx, max(h.bl_6m_cost_rx_anti_dm) as bl_6m_cost_rx_anti_dm
from (select distinct patid, sum(std_cost) as bl_6m_cost_med from ty33_bl_6m_med group by patid) a
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_inp from ty33_bl_6m_med where pos='21' group by patid) b on a.patid=b.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_er from ty33_bl_6m_med where pos='23' group by patid) c on a.patid=c.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm from ty33_bl_6m_med_dm group by patid) d on a.patid=d.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_inp from ty33_bl_6m_med_dm where pos='21' group by patid) e on a.patid=e.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_med_dm_er from ty33_bl_6m_med_dm where pos='23' group by patid) f on a.patid=f.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx from ty33_bl_6m_rx group by patid) g on a.patid=g.patid
        full join (select distinct patid, sum(std_cost) as bl_6m_cost_rx_anti_dm from ty33_bl_6m_rx_anti_dm group by patid) h on a.patid=h.patid
group by patid1
order by patid1
;

select count(*) as n_obs, count(distinct patid1) as n_pat
from ty33_pat_bl_6m_cost
;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("Select * from ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_bl_6m_cost")
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC df = spark.sql("Select * from ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/FileStore/tables/ty33_pat_fu_6m_cost")
-- MAGIC
-- MAGIC display(df)
