-- Databricks notebook source
drop table if exists ac_ehr_dx_t1d_hypo;
create or replace table ac_ehr_dx_t1d_hypo as
select distinct a.ptid, a.encid, diag_date, DIAGNOSIS_CD, DIAGNOSIS_STATUS, DIAGNOSIS_CD_TYPE,
b.Disease, b.dx_name, b.description, b.weight, b.weight_old
from ac_ehr_diag_202211 a join ty00_all_dx_comorb b
on a.DIAGNOSIS_CD=b.code
where
 b.dx_name in ('HYPO','T1DM') and diag_date>='2021-01-01' and diag_date<='2021-12-31'
and DIAGNOSIS_STATUS='Diagnosis of';

-- COMMAND ----------

select distinct * from ac_ehr_dx_t1d_hypo
order by ptid, diag_date

-- create or replace table ac_ehr_t1d_202211_diag_indx as
-- select distinct ptid, year(diag_date) as yr_indx, diag_date as dt_1st_dx_t1d, encid, rank
-- from (select *, dense_rank() over (partition by ptid order by diag_date) as rank
--       from ac_ehr_t1d_202211_diag_1  )
-- where rank<=1
-- order by ptid
-- ;


-- COMMAND ----------

select count(distinct ptid) from ac_ehr_t1d_202211_diag_indx

-- COMMAND ----------

-- MAGIC %md #### Checking HbA1c values for T1D patients

-- COMMAND ----------

create or replace  table ac_ehr_t1d_lab_202211_1 as
select distinct a.ptid, a.encid, a.test_type,a.test_name, a.test_result, a.relative_indicator, a.result_unit, 
a.normal_range, a.evaluated_for_range, a.value_within_range, a.result_date, 
coalesce(a.result_date,a.collected_date,a.order_date) as service_date
from ac_labs_ehr_202211 a 
where test_name='Hemoglobin A1C' and 
upper(test_name) not in ('',' ','UNK','NONE','UNKNOWN','NOCPT') ;

-- COMMAND ----------

select distinct * from ac_ehr_t1d_lab_202211_1
order by ptid, service_date

-- COMMAND ----------

drop table if exists ac_ehr_t1d_lab_202211_2;
create or replace table ac_ehr_t1d_lab_202211_2 as
select distinct * from ac_ehr_t1d_lab_202211_1
where '2021-01-01'<=service_date and service_date<='2021-12-31'
order by ptid, service_date;

-- COMMAND ----------

create or replace table ac_t1d_a1c_value_tst as

select distinct test_result, result_unit from ac_ehr_t1d_lab_202211_2;

-- COMMAND ----------

-- select distinct * from ac_ehr_t1d_lab_202211_2
-- where result_unit='mg/l';

select distinct * from ac_t1d_a1c_value_tst
where result_unit='2-2-2021';

-- hr
-- cm
-- #
-- mm




-- COMMAND ----------

create or replace table ac_ehr_t1d_lab_202211_3 as
select distinct * from ac_ehr_t1d_lab_202211_2
where result_unit  not in ('hr',
'cm',
'#',
'mm'
)
order by ptid, service_date;

select distinct * from ac_ehr_t1d_lab_202211_3;

-- COMMAND ----------

create or replace table ac_ehr_t1d_lab_202211_4 as
select distinct *,
case when test_result ="-7.5" then 7.5
when test_result =">^14.3" then 14.3
when test_result ="<^4.2" then 4.2
when test_result ="7.0&" then 7
when test_result ="<^4.3" then 4.3
when test_result ="9.7+" then 9.7
when test_result ="8/0.7" then 11.42
when test_result ="6.1." then 6.1
when test_result ="6.8%+" then 6.8
when test_result ="8.6." then 8.6
when test_result ="14.4%f" then 14.4
when test_result ="15.5" then 15.5
when test_result ="7.8h" then 7.8
when test_result ="7.3." then 7.3
when test_result ="6;0" then 6
when test_result ="10/6%" then 1.66
when test_result ="10/6" then 1.66
when test_result ="9.6.%" then 9.6
when test_result ="5;6%" then 5.6
when test_result ="6.3%+" then 6.3
when test_result ="6.0a" then 6
when test_result ="6.p0" then 6
when test_result ="7;2" then 7.2
when test_result ="7;8%" then 7.8
when test_result ="12;8" then 12.8
when test_result ="7;1" then 7.1
when test_result ="8.8." then 8.8
when test_result ="5;4" then 5.4
when test_result ="5;5" then 5.5
when test_result ="8;1" then 8.1
when test_result ="9.3%c" then 9.3
when test_result ="5.5." then 5.5
when test_result ="7;4" then 7.4
when test_result ="8;2" then 8.2
when test_result ="6;4" then 6.4
when test_result ="5.3.%" then 5.3
when test_result ="7.o" then 7
when test_result ="6.6." then 6.6
when test_result ="7+" then 7
when test_result ="6.6%." then 6.6
when test_result ="15+" then 15
when test_result ="8.3." then 8.3
when test_result ="6.6+" then 6.6
when test_result ="6;2" then 6.2
when test_result ="6;8" then 6.8
when test_result ="6.9.%" then 6.9
when test_result ="7.4%$" then 7.4
when test_result ="6.3." then 6.3
when test_result ="6;2%" then 6.2
when test_result ="5..3" then 5.3
when test_result ="10.1." then 10.1
when test_result ="7.5h" then 7.5
when test_result ="6.2%+" then 6.2
when test_result ="6.4f" then 6.4
when test_result ="4.5." then 4.5
when test_result ="11.5f" then 11.5
when test_result ="7.1!" then 7.1
when test_result ="7.8%+" then 7.8
when test_result ="6.0." then 6
when test_result ="5;4" then 5.4
when test_result ="12.7%+" then 12.7
when test_result ="8.0%+" then 8
when test_result ="4.9n" then 4.9
when test_result ="6;7%" then 6.7
when test_result ="-1" then 1
when test_result ="5.5n" then 5.5
when test_result ="5.3." then 5.3
when test_result ="5..6" then 5.6
when test_result ="0.14" then 14
when test_result ="11.5+" then 11.5
when test_result ="12.8+" then 12.8
when test_result ="14+" then 14
when test_result ="-14" then 14
when test_result ="6.7`" then 6.7
when test_result ="6.3+" then 6.3
when test_result ="6.8+" then 6.8
when test_result ="7.0+" then 7
when test_result ="10.2f" then 10.2
when test_result ="11.8/" then 11.8
when test_result ="5.0+" then 5
when test_result ="13.8#" then 13.8
when test_result ="7.0f" then 7
when test_result ="6.5+" then 6.5
when test_result ="6.1+" then 6.1
when test_result ="6.2+" then 6.2
when test_result ="8.0+" then 8
when test_result ="5./ %" then 5
when test_result ="5.3f" then 5.3
when test_result ="6.4+" then 6.4
when test_result ="7.7+" then 7.7
when test_result ="9?" then 9
when test_result ="14%+" then 14
when test_result ="5.2." then 5.2
when test_result ="7.8+" then 7.8
when test_result ="9.1%+" then 9.1
when test_result ="7.1+" then 7.1
when test_result ="9.5+" then 9.5
when test_result ="7/7%" then 1
when test_result =".6.7%" then 6.7
when test_result =">14+" then 14
when test_result ="9.8hh" then 9.8
when test_result ="12.3.%" then 12.3
when test_result ="6.5h" then 6.5
when test_result ="5'9" then 5.9
when test_result ="5/6%" then 0.83
when test_result ="5;2" then 5.2
when test_result ="8.9`" then 8.9
when test_result ="6.8." then 6.8
when test_result ="5;8%" then 5.8
when test_result =".>14" then 14
when test_result ="9.5." then 9.5
when test_result ="8.3h" then 8.3
when test_result ="9.3hh" then 9.3
when test_result ="7.4h" then 7.4
when test_result ="8;7%" then 8.7
when test_result ="11;7%" then 11.7
when test_result ="6;7" then 6.7
when test_result ="12.5+" then 12.5
when test_result ="-5.4000001" then 5.4
when test_result ="-8.1000004" then 8.1
when test_result ="5;8" then 5.8
when test_result ="8.9.%" then 8.9
when test_result ="10;9" then 10.9
when test_result ="14.0%+" then 14
when test_result ="7.6t" then 7.6
when test_result ="12..0" then 12
when test_result ="7;4%" then 7.4
when test_result ="6.o%" then 6
when test_result ="8.8.%" then 8.8
when test_result ="13+" then 13
when test_result ="7;1%" then 7.1
when test_result ="5;7" then 5.7
when test_result ="6.6h" then 6.6
when test_result ="-6" then 6
when test_result ="0.07" then 7
when test_result ="9;4" then 9.4
when test_result ="0.05" then 5
when test_result ="6.3&" then 6.3
when test_result ="6.7%+" then 6.7
when test_result ="7;4" then 7.4
when test_result ="12.2+" then 12.2
when test_result ="4.7&" then 4.7
when test_result ="11.o%" then 11
when test_result ="5.5n" then 5.5
when test_result ="7;5" then 7.5
when test_result ="14.0+" then 14
when test_result ="6;5%" then 6.5
when test_result ="5;3" then 5.3
when test_result ="6.6.%" then 6.6
when test_result =".6.6" then 6.6
when test_result ="6.3d%" then 6.3
when test_result ="7.2%+" then 7.2
when test_result ="6;1%" then 6.1
when test_result ="8.9%+" then 8.9
when test_result ="6.5" then 6.5
when test_result ="6.-0" then 6
when test_result ="6.8f" then 6.8
when test_result ="5.5f" then 5.5
when test_result ="7.1f" then 7.1
when test_result ="5.2n" then 5.2
when test_result ="5.6n" then 5.6
when test_result ="11.2a" then 11.2
when test_result ="6.2a" then 6.2
when test_result ="4.8n" then 4.8
when test_result ="8.3." then 8.3
when test_result ="5.9." then 5.9
when test_result ="11.4+" then 11.4
when test_result ="8:4%" then 8.4
when test_result ="-5.5999999" then 5.6
when test_result ="#106" then 106
when test_result ="4.3 - 6.0 %" then 6
when test_result ="10.1%+" then 10.1
when test_result ="12.4+" then 12.4
when test_result ="6.4%+" then 6.4
when test_result ="6..8" then 6.8
when test_result ="5.00%" then 5
when test_result ="7.4a" then 7.4
when test_result ="8.1a" then 8.1
when test_result ="8.0a" then 8
when test_result ="8/7%" then 1.14
when test_result ="11. 8" then 11.8
when test_result ="7. 9" then 7.9
when test_result ="14.%" then 14
when test_result ="10/8" then 1.25
when test_result ="7.%" then 7
when test_result ="7/1" then 7
when test_result ="10/1" then 10
when test_result ="7/3" then 2.33
when test_result ="5.%" then 5

else test_result end as test_result_new 
from ac_ehr_t1d_lab_202211_3
where test_result<>''
order by ptid, service_date;

select distinct * from ac_ehr_t1d_lab_202211_4
where test_result ="10/6"
order by ptid, service_date;










-- COMMAND ----------

describe table ac_ehr_t1d_lab_202211_5;

-- COMMAND ----------

create or replace table ac_ehr_t1d_lab_202211_5 as
select distinct *, float(test_result_new) as test_result_1
from ac_ehr_t1d_lab_202211_4
order by ptid, service_date;

select distinct * from ac_ehr_t1d_lab_202211_5;

-- select count(distinct ptid) from ac_ehr_t1d_lab_202211_5;
create or replace table ac_ehr_t1d_lab_202211_6 as
select distinct *, case when result_unit='mg/dl' then (test_result_1 + 46.7)/28.7
when result_unit='g/dl' then (((test_result_1*1000) + 46.7)/28.7)
else test_result_1 end as test_result_updated
from ac_ehr_t1d_lab_202211_5
order by ptid, service_date;


select distinct * from ac_ehr_t1d_lab_202211_6;

-- COMMAND ----------

select distinct *from ac_ehr_t1d_lab_202211_6
where ptid='PT078153139'

order by ptid, service_date;



-- COMMAND ----------

-- create or replace table ac_lab_a1c_ehr_max_21_value as
-- select distinct ptid, max(test_result_updated) as value from ac_ehr_t1d_lab_202211_6
-- where '2021-01-01'<=service_date and service_date<='2021-12-31'
-- group by ptid
-- order by 1;

-- select distinct * from ac_lab_a1c_ehr_max_21_value
-- order by 1;

-- create or replace table ac_ehr_t1d_lab_202211_7 as
-- select distinct a.*,b.value from ac_ehr_t1d_lab_202211_6 a
-- inner join ac_lab_a1c_ehr_max_21_value b on a.ptid=b.ptid
-- where '2021-01-01'<=a.service_date and a.service_date<='2021-12-31'
-- order by a.ptid, a.service_date;

select distinct * from ac_ehr_t1d_lab_202211_7
order by ptid, service_date;

-- COMMAND ----------

drop table if exists ac_ehr_t1dm_etc;

create table ac_ehr_t1dm_etc as
select distinct a.ptid, a.dt_1st_dx_t1dm, a.n_dx_t1dm, b.dt_1st_dx_hypo, b.n_dx_hypo, c.n_lb_a1c_ge75, d.n_lb_a1c_ge9, e.n_lb_a1c_le7, m.n_lb_a1c_ge7, f.n_lb_a1c, g.n_rx_insulin, h.FIRST_MONTH_ACTIVE_new, h.LAST_MONTH_ACTIVE_new
, case when isnotnull(e.n_lb_a1c_le7) then e.n_lb_a1c_le7/f.n_lb_a1c
       else null end as pct_a1c_in_range
from (select distinct ptid, min(diag_date) as dt_1st_dx_t1dm, count(distinct diag_date) as n_dx_t1dm from ac_ehr_dx_t1d_hypo where dx_name='T1DM' group by ptid) a
left join (select distinct ptid, min(diag_date) as dt_1st_dx_hypo, count(distinct diag_date) as n_dx_hypo from ac_ehr_dx_t1d_hypo where dx_name='HYPO' group by ptid) b on a.ptid=b.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_ge75 from ac_ehr_t1d_lab_202211_7 where '2021-01-01'<=service_date and service_date<='2021-12-31' and value>=7.5 group by ptid) c on a.ptid=c.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_ge9  from ac_ehr_t1d_lab_202211_7 where '2021-01-01'<=service_date and service_date<='2021-12-31' and value>=9   group by ptid) d on a.ptid=d.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_le7  from ac_ehr_t1d_lab_202211_7 where '2021-01-01'<=service_date and service_date<='2021-12-31' and test_result_updated<=7 and isnotnull(test_result_updated) group by ptid) e on a.ptid=e.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_ge7  from ac_ehr_t1d_lab_202211_7 where '2021-01-01'<=service_date and service_date<='2021-12-31' and value>=7 and isnotnull(value) group by ptid) m on a.ptid=m.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c      from ac_ehr_t1d_lab_202211_7 where '2021-01-01'<=service_date and service_date<='2021-12-31' and isnotnull(test_result_updated) group by ptid) f on a.ptid=f.ptid
-- left join (select distinct patid, count(distinct fill_dt) as n_rx_insulin from ty37_rx_anti_dm where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31' and rx_type in ('Basal','Bolus','PreMix') group by patid) g on a.patid=g.patid
left join (select distinct ptid, count(distinct rxdate) as n_rx_insulin from ac_ehr_insulin_RAI_med_admn_WRx where '2021-01-01'<=rxdate and rxdate<='2021-12-31' group by ptid) g on a.ptid=g.ptid
left join (select distinct ptid, FIRST_MONTH_ACTIVE_new, LAST_MONTH_ACTIVE_new from ac_Patient_ehr_202211_t1d_1) h on a.ptid=h.ptid and a.dt_1st_dx_t1dm between h.FIRST_MONTH_ACTIVE_new and h.LAST_MONTH_ACTIVE_new
order by a.ptid
;

select * from ac_ehr_t1dm_etc;

-- COMMAND ----------

drop table if exists ac_ehr_t1dm_etc;

create table ac_ehr_t1dm_etc as
select distinct a.ptid, a.dt_1st_dx_t1dm, a.n_dx_t1dm, b.dt_1st_dx_hypo, b.n_dx_hypo, c.n_lb_a1c_ge75, d.n_lb_a1c_ge9, e.n_lb_a1c_le7, m.n_lb_a1c_ge7, f.n_lb_a1c, g.n_rx_insulin, h.FIRST_MONTH_ACTIVE_new, h.LAST_MONTH_ACTIVE_new
, case when isnotnull(e.n_lb_a1c_le7) then e.n_lb_a1c_le7/f.n_lb_a1c
       else null end as pct_a1c_in_range
from (select distinct ptid, min(diag_date) as dt_1st_dx_t1dm, count(distinct diag_date) as n_dx_t1dm from ac_ehr_dx_t1d_hypo where dx_name='T1DM' group by ptid) a
left join (select distinct ptid, min(diag_date) as dt_1st_dx_hypo, count(distinct diag_date) as n_dx_hypo from ac_ehr_dx_t1d_hypo where dx_name='HYPO' group by ptid) b on a.ptid=b.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_ge75 from ac_ehr_t1d_lab_202211_6 where '2021-01-01'<=service_date and service_date<='2021-12-31' and test_result_updated>=7.5 group by ptid) c on a.ptid=c.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_ge9  from ac_ehr_t1d_lab_202211_6 where '2021-01-01'<=service_date and service_date<='2021-12-31' and test_result_updated>=9   group by ptid) d on a.ptid=d.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_le7  from ac_ehr_t1d_lab_202211_6 where '2021-01-01'<=service_date and service_date<='2021-12-31' and test_result_updated<=7 and isnotnull(test_result_updated) group by ptid) e on a.ptid=e.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c_ge7  from ac_ehr_t1d_lab_202211_6 where '2021-01-01'<=service_date and service_date<='2021-12-31' and test_result_updated>=7 and isnotnull(test_result_updated) group by ptid) m on a.ptid=m.ptid
left join (select distinct ptid, count(distinct service_date) as n_lb_a1c      from ac_ehr_t1d_lab_202211_6 where '2021-01-01'<=service_date and service_date<='2021-12-31' and isnotnull(test_result_updated) group by ptid) f on a.ptid=f.ptid
-- left join (select distinct patid, count(distinct fill_dt) as n_rx_insulin from ty37_rx_anti_dm where '2021-01-01'<=fill_dt and fill_dt<='2021-12-31' and rx_type in ('Basal','Bolus','PreMix') group by patid) g on a.patid=g.patid
left join (select distinct ptid, count(distinct rxdate) as n_rx_insulin from ac_ehr_insulin_RAI_med_admn_WRx where '2021-01-01'<=rxdate and rxdate<='2021-12-31' group by ptid) g on a.ptid=g.ptid
left join (select distinct ptid, FIRST_MONTH_ACTIVE_new, LAST_MONTH_ACTIVE_new from ac_Patient_ehr_202211_t1d_1) h on a.ptid=h.ptid and a.dt_1st_dx_t1dm between h.FIRST_MONTH_ACTIVE_new and h.LAST_MONTH_ACTIVE_new
order by a.ptid
;

select * from ac_ehr_t1dm_etc;

-- COMMAND ----------

describe table ac_ehr_t1dm_etc;

-- COMMAND ----------

drop table if exists ac_ehr_t1dm_freq;

create table ac_ehr_t1dm_freq as
select '1. # of people with Type 1 diabetes' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc
union
select '2. # of people with Type 1 diabetes continued enrolled in year 2021' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '2.1. Have A1C >=7%' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_lb_a1c_ge7) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '3. Have A1C >=7.5%' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_lb_a1c_ge75) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '4. Have A1C >=9.0%' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_lb_a1c_ge9 ) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '5. Have >3 severe hypoglycemia episodes in 1 year' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where n_dx_hypo>3 and isnotnull(n_lb_a1c) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '6. Have A1c' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_lb_a1c)  and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '6a. Have a time in range <=60%' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_lb_a1c_le7) and pct_a1c_in_range<0.6 and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '7. Using a pump' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_rx_insulin) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '8. Using a pump with A1C >=7.5%' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_rx_insulin) and isnotnull(n_lb_a1c_ge75) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '9. Using a pump with A1C >=9.0%' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_rx_insulin) and isnotnull(n_lb_a1c_ge9 ) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
union
select '10. Using a pump with A1C >=7.0%' as Cat, count(distinct ptid) as N, min(dt_1st_dx_t1dm) as dt_t1dm_start, max(dt_1st_dx_t1dm) as dt_t1dm_end from ac_ehr_t1dm_etc where isnotnull(n_rx_insulin) and isnotnull(n_lb_a1c_ge7 ) and FIRST_MONTH_ACTIVE_new<='2021-01-01' and '2021-12-31'<=LAST_MONTH_ACTIVE_new
order by cat
;

select * from ac_ehr_t1dm_freq;
