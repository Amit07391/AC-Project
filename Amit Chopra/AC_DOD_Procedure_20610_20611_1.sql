-- Databricks notebook source
create or replace table ac_dod_20610_20611_test_22 as
select distinct a.*, c.bus, c.PRODUCT, c.ELIGEFF, c.ELIGEND from ac_dod_2307_med_claims a
left join ac_dod_2307_member_enrol c on a.PATID=c.PATID
where a.FST_DT>='2022-01-01' and a.FST_DT<='2022-12-31' and a.proc_cd in ('20610','20611') and a.PAID_STATUS='P' 
order by a.patid, a.FST_DT;

select distinct * from ac_dod_20610_20611_test_22
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_20610_20611_test_22_ins_type as
select distinct patid,max(eligend) as max_end_dt from ac_dod_20610_20611_test_22
group by 1
order by 1;

select distinct * from ac_dod_20610_20611_test_22_ins_type
order by 1;

-- COMMAND ----------

create or replace table ac_dod_20610_20611_test_22_2 as
select distinct a.patid, bus, product from ac_dod_20610_20611_test_22 a
inner join ac_dod_20610_20611_test_22_ins_type b on a.patid=b.patid and a.ELIGEND=b.max_end_dt
order by a.patid;

select distinct * from ac_dod_20610_20611_test_22_2
order by patid;

-- COMMAND ----------

create or replace table ac_dod_20610_20611_test_22_final as
select distinct a.PATID,	PAT_PLANID,	ADMIT_CHAN,	ADMIT_TYPE,	BILL_PROV,	CHARGE,	CLMID,	CLMSEQ,	COB,	COINS,	CONF_ID,	COPAY,	DEDUCT,	DRG,	DSTATUS,	ENCTR,	FST_DT,	HCCC,	ICD_FLAG,	LOC_CD,	LST_DT,	NDC,	PAID_DT,	PAID_STATUS,	POS,	PROC_CD,	PROCMOD,	PROV,	PROV_PAR,	PROVCAT,	REFER_PROV,	RVNU_CD,	SERVICE_PROV,	STD_COST,	STD_COST_YR,	TOS_CD,	UNITS,	EXTRACT_YM,	VERSION,	ALT_UNITS,	BILL_TYPE,	NDC_UOM,	NDC_QTY,	OP_VISIT_ID,	PROCMOD2,	PROCMOD3,	PROCMOD4,	TOS_EXT,	ELIGEFF,	ELIGEND, b.bus, b.product from ac_dod_20610_20611_test_22 a
inner join ac_dod_20610_20611_test_22_2 b on a.patid=b.patid
order by a.patid, a.FST_DT;

select distinct * from ac_dod_20610_20611_test_22_final
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_20610_test_2022 as
select distinct a.patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost, PROC_CD, a.BUS, a.PRODUCT  from ac_dod_20610_20611_test_22_final a
where a.PROC_CD='20610'
order by a.patid, fst_dt;

select distinct * from ac_dod_20610_test_2022
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_dod_20610_test_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_20610_test_2022
group by patid,2,3
order by 1,2,3;

select distinct * from ac_dod_20610_test_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_no_afib_liver_lab_cost

-- COMMAND ----------

select distinct * from ac_dod_20610_test_2022
where patid in ('33003282180')


-- COMMAND ----------

select '201610' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost  from ac_dod_20610_test_cost
group by bus, product
order by bus, product;

-- COMMAND ----------

create or replace table ac_dod_20611_test_2022 as
select distinct a.patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost, PROC_CD, a.BUS, a.PRODUCT  from ac_dod_20610_20611_test_22_final a
where a.PROC_CD='20611'
order by a.patid, fst_dt;

select distinct * from ac_dod_20611_test_2022
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_dod_20611_test_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_20611_test_2022
group by patid,2,3
order by 1,2,3;

select distinct * from ac_dod_20611_test_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_no_afib_liver_lab_cost

-- COMMAND ----------

select '20611' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost  from ac_dod_20611_test_cost
group by bus, product
order by bus, product;

-- COMMAND ----------

drop table if exists ac_dod_20610_test_cost_stat1;
create table default.ac_dod_20610_test_cost_stat1 as
select bus, product, 
charge,
count(*) as charge_freq
from default.ac_dod_20610_test_cost
group by bus, product,
charge
order by 4 desc;

drop table if exists ac_dod_20610_test_cost_stat2;
create table default.ac_dod_20610_test_cost_stat2 as
select bus, product, 
copay,
count(*) as copay_freq
from default.ac_dod_20610_test_cost
group by bus, product,
copay
order by 4 desc;

drop table if exists ac_dod_20610_test_cost_stat3;
create table default.ac_dod_20610_test_cost_stat3 as
select bus, product, 
ded,
count(*) as ded_freq
from default.ac_dod_20610_test_cost
group by bus, product,
ded
order by 4 desc;

drop table if exists ac_dod_20610_test_cost_stat4;
create table default.ac_dod_20610_test_cost_stat4 as
select bus, product, 
std_cost,
count(*) as cost_freq
from default.ac_dod_20610_test_cost
group by bus, product,
std_cost
order by 4 desc;

drop table if exists ac_dod_20610_test_cost_stat5;
create table default.ac_dod_20610_test_cost_stat5 as
select bus, product, 
coins,
count(*) as coins_freq
from default.ac_dod_20610_test_cost
group by bus, product,
coins
order by 4 desc;






-- COMMAND ----------

drop table if exists ac_dod_proc_20610_cost_stats;
create table ac_dod_proc_20610_cost_stats as
 
(select a.*, 'charge' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by charge_freq desc) as row_num
from default.ac_dod_20610_test_cost_stat1 a) a
where row_num<=3)
 
union all
 
(select a.*, 'copay' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by copay_freq desc) as row_num
from default.ac_dod_20610_test_cost_stat2 a) a
where row_num<=3)
 
union all
 
(select a.*, 'deductible' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by ded_freq desc) as row_num
from default.ac_dod_20610_test_cost_stat3 a) a
where row_num<=3)
 
union all
 
(select a.*, 'std_cost' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by cost_freq desc) as row_num
from default.ac_dod_20610_test_cost_stat4 a) a
where row_num<=3)
 
union all
 
(select a.*, 'coins' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by coins_freq desc) as row_num
from default.ac_dod_20610_test_cost_stat5 a) a
where row_num<=3);


Select * from ac_dod_proc_20610_cost_stats;


-- COMMAND ----------

drop table if exists ac_dod_20611_test_cost_stat1;
create table default.ac_dod_20611_test_cost_stat1 as
select bus, product, 
charge,
count(*) as charge_freq
from default.ac_dod_20611_test_cost
group by bus, product,
charge
order by 4 desc;

drop table if exists ac_dod_20611_test_cost_stat2;
create table default.ac_dod_20611_test_cost_stat2 as
select bus, product, 
copay,
count(*) as copay_freq
from default.ac_dod_20611_test_cost
group by bus, product,
copay
order by 4 desc;

drop table if exists ac_dod_20611_test_cost_stat3;
create table default.ac_dod_20611_test_cost_stat3 as
select bus, product, 
ded,
count(*) as ded_freq
from default.ac_dod_20611_test_cost
group by bus, product,
ded
order by 4 desc;

drop table if exists ac_dod_20611_test_cost_stat4;
create table default.ac_dod_20611_test_cost_stat4 as
select bus, product, 
std_cost,
count(*) as cost_freq
from default.ac_dod_20611_test_cost
group by bus, product,
std_cost
order by 4 desc;

drop table if exists ac_dod_20611_test_cost_stat5;
create table default.ac_dod_20611_test_cost_stat5 as
select bus, product, 
coins,
count(*) as coins_freq
from default.ac_dod_20611_test_cost
group by bus, product,
coins
order by 4 desc;






-- COMMAND ----------

drop table if exists ac_dod_proc_20611_cost_stats;
create table ac_dod_proc_20611_cost_stats as
 
(select a.*, 'charge' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by charge_freq desc) as row_num
from default.ac_dod_20611_test_cost_stat1 a) a
where row_num<=3)
 
union all
 
(select a.*, 'copay' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by copay_freq desc) as row_num
from default.ac_dod_20611_test_cost_stat2 a) a
where row_num<=3)
 
union all
 
(select a.*, 'deductible' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by ded_freq desc) as row_num
from default.ac_dod_20611_test_cost_stat3 a) a
where row_num<=3)
 
union all
 
(select a.*, 'std_cost' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by cost_freq desc) as row_num
from default.ac_dod_20611_test_cost_stat4 a) a
where row_num<=3)
 
union all
 
(select a.*, 'coins' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by coins_freq desc) as row_num
from default.ac_dod_20611_test_cost_stat5 a) a
where row_num<=3);


Select * from ac_dod_proc_20611_cost_stats;

