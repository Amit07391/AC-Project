-- Databricks notebook source
-- create or replace table ac_dod_afib_lab_test_Liver_2022 as
-- select distinct a.patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost, PROC_CD, b.BUS, b.PRODUCT, b.ELIGEFF, b.ELIGEND  from ac_dod_afib_lab_test_22 a
-- left join ac_dod_2307_member_enrol b on a.PATID=b.PATID
-- where organ='Liver' and PAID_STATUS='P' 
-- order by a.patid, fst_dt;

select distinct * from ac_dod_afib_lab_test_Liver_2022
order by patid, fst_dt;

-- COMMAND ----------

select bus, product, count(distinct a.patid), median(charge), median(copay), median(deduct), median(coins), median(std_cost) from ac_dod_afib_lab_test_Liver_2022 a
inner join ac_dod_dx_afib_22 b on a.patid=b.patid
group by 1,2
order by 1,2;

-- COMMAND ----------


create or replace table ac_afib_liver_lab_cost as
select distinct a.patid, a.BUS, a.PRODUCT, count(distinct a.fst_dt) as n_proc, sum(copay) as Liver_copay, sum(deduct) as Liver_ded, sum(coins) as Liver_coins,
sum(std_cost) as Liver_std_cost from ac_dod_afib_lab_test_Liver_2022 a
inner join ac_dod_dx_afib_22 b on a.patid=b.patid  group by a.patid, a.BUS, a.PRODUCT;

select distinct * from ac_afib_liver_lab_cost
order by patid;

-- select sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_liver_lab_cost

-- COMMAND ----------

select bus, product, sum(n_proc), count(distinct patid),  median(liver_copay), median(Liver_ded), median(Liver_coins), median(Liver_std_cost) from ac_afib_liver_lab_cost
where
group by 1,2
order by 1,2;
