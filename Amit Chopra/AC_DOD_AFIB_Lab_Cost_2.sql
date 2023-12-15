-- Databricks notebook source
drop table if exists ac_afib_Z79899_lab_cost_stat1;
create table default.ac_afib_Z79899_lab_cost_stat1 as
select bus, product, 
charge,
count(*) as charge_freq
from default.ac_afib_Z79899_lab_cost
group by bus, product,
charge
order by 4 desc;

drop table if exists ac_afib_Z79899_lab_cost_stat2;
create table default.ac_afib_Z79899_lab_cost_stat2 as
select bus, product, 
copay,
count(*) as copay_freq
from default.ac_afib_Z79899_lab_cost
group by bus, product,
copay
order by 4 desc;

drop table if exists ac_afib_Z79899_lab_cost_stat3;
create table default.ac_afib_Z79899_lab_cost_stat3 as
select bus, product, 
ded,
count(*) as ded_freq
from default.ac_afib_Z79899_lab_cost
group by bus, product,
ded
order by 4 desc;

drop table if exists ac_afib_Z79899_lab_cost_stat4;
create table default.ac_afib_Z79899_lab_cost_stat4 as
select bus, product, 
std_cost,
count(*) as cost_freq
from default.ac_afib_Z79899_lab_cost
group by bus, product,
std_cost
order by 4 desc;

drop table if exists ac_afib_Z79899_lab_cost_stat5;
create table default.ac_afib_Z79899_lab_cost_stat5 as
select bus, product, 
coins,
count(*) as coins_freq
from default.ac_afib_Z79899_lab_cost
group by bus, product,
coins
order by 4 desc;






-- COMMAND ----------

drop table if exists ac_afib_ICD_lab_cost_stats;
create table ac_afib_ICD_lab_cost_stats as
 
(select a.*, 'charge' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by charge_freq desc) as row_num
from default.ac_afib_Z79899_lab_cost_stat1 a) a
where row_num<=3)
 
union all
 
(select a.*, 'copay' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by copay_freq desc) as row_num
from default.ac_afib_Z79899_lab_cost_stat2 a) a
where row_num<=3)
 
union all
 
(select a.*, 'deductible' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by ded_freq desc) as row_num
from default.ac_afib_Z79899_lab_cost_stat3 a) a
where row_num<=3)
 
union all
 
(select a.*, 'std_cost' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by cost_freq desc) as row_num
from default.ac_afib_Z79899_lab_cost_stat4 a) a
where row_num<=3)
 
union all
 
(select a.*, 'coins' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by coins_freq desc) as row_num
from default.ac_afib_Z79899_lab_cost_stat5 a) a
where row_num<=3);


Select * from ac_afib_ICD_lab_cost_stats;


-- COMMAND ----------

drop table if exists ac_afib_Z0000_lab_cost_stat1;
create table default.ac_afib_Z0000_lab_cost_stat1 as
select bus, product, 
charge,
count(*) as charge_freq
from default.ac_afib_Z0000_lab_cost
group by bus, product,
charge
order by 4 desc;

drop table if exists ac_afib_Z0000_lab_cost_stat2;
create table default.ac_afib_Z0000_lab_cost_stat2 as
select bus, product, 
copay,
count(*) as copay_freq
from default.ac_afib_Z0000_lab_cost
group by bus, product,
copay
order by 4 desc;

drop table if exists ac_afib_Z0000_lab_cost_stat3;
create table ac_afib_Z0000_lab_cost_stat3 as
select bus, product, 
ded,
count(*) as ded_freq
from default.ac_afib_Z0000_lab_cost
group by bus, product,
ded
order by 4 desc;

drop table if exists ac_afib_Z0000_lab_cost_stat4;
create table default.ac_afib_Z0000_lab_cost_stat4 as
select bus, product, 
std_cost,
count(*) as cost_freq
from default.ac_afib_Z0000_lab_cost
group by bus, product,
std_cost
order by 4 desc;

drop table if exists ac_afib_Z0000_lab_cost_stat5;
create table default.ac_afib_Z0000_lab_cost_stat5 as
select bus, product, 
coins,
count(*) as coins_freq
from default.ac_afib_Z0000_lab_cost
group by bus, product,
coins
order by 4 desc;






-- COMMAND ----------

drop table if exists ac_afib_ICD_Z0000_lab_cost_stats;
create table ac_afib_ICD_Z0000_lab_cost_stats as
 
(select a.*, 'charge' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by charge_freq desc) as row_num
from default.ac_afib_Z0000_lab_cost_stat1 a) a
where row_num<=3)
 
union all
 
(select a.*, 'copay' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by copay_freq desc) as row_num
from default.ac_afib_Z0000_lab_cost_stat2 a) a
where row_num<=3)
 
union all
 
(select a.*, 'deductible' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by ded_freq desc) as row_num
from default.ac_afib_Z0000_lab_cost_stat3 a) a
where row_num<=3)
 
union all
 
(select a.*, 'std_cost' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by cost_freq desc) as row_num
from default.ac_afib_Z0000_lab_cost_stat4 a) a
where row_num<=3)
 
union all
 
(select a.*, 'coins' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by coins_freq desc) as row_num
from default.ac_afib_Z0000_lab_cost_stat5 a) a
where row_num<=3);


Select * from ac_afib_ICD_Z0000_lab_cost_stats;


-- COMMAND ----------

drop table if exists ac_afib_Z139_lab_cost_stat1;
create table default.ac_afib_Z139_lab_cost_stat1 as
select bus, product, 
charge,
count(*) as charge_freq
from default.ac_afib_Z139_lab_cost
group by bus, product,
charge
order by 4 desc;

drop table if exists ac_afib_Z139_lab_cost_stat2;
create table default.ac_afib_Z139_lab_cost_stat2 as
select bus, product, 
copay,
count(*) as copay_freq
from default.ac_afib_Z139_lab_cost
group by bus, product,
copay
order by 4 desc;

drop table if exists ac_afib_Z139_lab_cost_stat3;
create table ac_afib_Z139_lab_cost_stat3 as
select bus, product, 
ded,
count(*) as ded_freq
from default.ac_afib_Z139_lab_cost
group by bus, product,
ded
order by 4 desc;

drop table if exists ac_afib_Z139_lab_cost_stat4;
create table default.ac_afib_Z139_lab_cost_stat4 as
select bus, product, 
std_cost,
count(*) as cost_freq
from default.ac_afib_Z139_lab_cost
group by bus, product,
std_cost
order by 4 desc;

drop table if exists ac_afib_Z139_lab_cost_stat5;
create table default.ac_afib_Z139_lab_cost_stat5 as
select bus, product, 
coins,
count(*) as coins_freq
from default.ac_afib_Z139_lab_cost
group by bus, product,
coins
order by 4 desc;






-- COMMAND ----------

drop table if exists ac_afib_ICD_Z139_lab_cost_stats;
create table ac_afib_ICD_Z139_lab_cost_stats as
 
(select a.*, 'charge' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by charge_freq desc) as row_num
from default.ac_afib_Z139_lab_cost_stat1 a) a
where row_num<=3)
 
union all
 
(select a.*, 'copay' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by copay_freq desc) as row_num
from default.ac_afib_Z139_lab_cost_stat2 a) a
where row_num<=3)
 
union all
 
(select a.*, 'deductible' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by ded_freq desc) as row_num
from default.ac_afib_Z139_lab_cost_stat3 a) a
where row_num<=3)
 
union all
 
(select a.*, 'std_cost' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by cost_freq desc) as row_num
from default.ac_afib_Z139_lab_cost_stat4 a) a
where row_num<=3)
 
union all
 
(select a.*, 'coins' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by coins_freq desc) as row_num
from default.ac_afib_Z139_lab_cost_stat5 a) a
where row_num<=3);


Select * from ac_afib_ICD_Z139_lab_cost_stats;


-- COMMAND ----------

drop table if exists ac_afib_Z0100_lab_cost_stat1;
create table default.ac_afib_Z0100_lab_cost_stat1 as
select bus, product, 
charge,
count(*) as charge_freq
from default.ac_afib_Z0100_lab_cost
group by bus, product,
charge
order by 4 desc;

drop table if exists ac_afib_Z0100_lab_cost_stat2;
create table default.ac_afib_Z0100_lab_cost_stat2 as
select bus, product, 
copay,
count(*) as copay_freq
from default.ac_afib_Z0100_lab_cost
group by bus, product,
copay
order by 4 desc;

drop table if exists ac_afib_Z0100_lab_cost_stat3;
create table ac_afib_Z0100_lab_cost_stat3 as
select bus, product, 
ded,
count(*) as ded_freq
from default.ac_afib_Z0100_lab_cost
group by bus, product,
ded
order by 4 desc;

drop table if exists ac_afib_Z0100_lab_cost_stat4;
create table default.ac_afib_Z0100_lab_cost_stat4 as
select bus, product, 
std_cost,
count(*) as cost_freq
from default.ac_afib_Z0100_lab_cost
group by bus, product,
std_cost
order by 4 desc;

drop table if exists ac_afib_Z0100_lab_cost_stat5;
create table default.ac_afib_Z0100_lab_cost_stat5 as
select bus, product, 
coins,
count(*) as coins_freq
from default.ac_afib_Z0100_lab_cost
group by bus, product,
coins
order by 4 desc;






-- COMMAND ----------

drop table if exists ac_afib_ICD_Z0100_lab_cost_stats;
create table ac_afib_ICD_Z0100_lab_cost_stats as
 
(select a.*, 'charge' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by charge_freq desc) as row_num
from default.ac_afib_Z0100_lab_cost_stat1 a) a
where row_num<=3)
 
union all
 
(select a.*, 'copay' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by copay_freq desc) as row_num
from default.ac_afib_Z0100_lab_cost_stat2 a) a
where row_num<=3)
 
union all
 
(select a.*, 'deductible' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by ded_freq desc) as row_num
from default.ac_afib_Z0100_lab_cost_stat3 a) a
where row_num<=3)
 
union all
 
(select a.*, 'std_cost' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by cost_freq desc) as row_num
from default.ac_afib_Z0100_lab_cost_stat4 a) a
where row_num<=3)
 
union all
 
(select a.*, 'coins' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by coins_freq desc) as row_num
from default.ac_afib_Z0100_lab_cost_stat5 a) a
where row_num<=3);


Select * from ac_afib_ICD_Z0100_lab_cost_stats;


-- COMMAND ----------

drop table if exists ac_afib_Z1383_lab_cost_stat1;
create table default.ac_afib_Z1383_lab_cost_stat1 as
select bus, product, 
charge,
count(*) as charge_freq
from default.ac_afib_Z1383_lab_cost
group by bus, product,
charge
order by 4 desc;

drop table if exists ac_afib_Z1383_lab_cost_stat2;
create table default.ac_afib_Z1383_lab_cost_stat2 as
select bus, product, 
copay,
count(*) as copay_freq
from default.ac_afib_Z1383_lab_cost
group by bus, product,
copay
order by 4 desc;

drop table if exists ac_afib_Z1383_lab_cost_stat3;
create table ac_afib_Z1383_lab_cost_stat3 as
select bus, product, 
ded,
count(*) as ded_freq
from default.ac_afib_Z1383_lab_cost
group by bus, product,
ded
order by 4 desc;

drop table if exists ac_afib_Z1383_lab_cost_stat4;
create table default.ac_afib_Z1383_lab_cost_stat4 as
select bus, product, 
std_cost,
count(*) as cost_freq
from default.ac_afib_Z1383_lab_cost
group by bus, product,
std_cost
order by 4 desc;

drop table if exists ac_afib_Z1383_lab_cost_stat5;
create table default.ac_afib_Z1383_lab_cost_stat5 as
select bus, product, 
coins,
count(*) as coins_freq
from default.ac_afib_Z1383_lab_cost
group by bus, product,
coins
order by 4 desc;






-- COMMAND ----------

drop table if exists ac_afib_ICD_Z1383_lab_cost_stats;
create table ac_afib_ICD_Z1383_lab_cost_stats as
 
(select a.*, 'charge' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by charge_freq desc) as row_num
from default.ac_afib_Z1383_lab_cost_stat1 a) a
where row_num<=3)
 
union all
 
(select a.*, 'copay' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by copay_freq desc) as row_num
from default.ac_afib_Z1383_lab_cost_stat2 a) a
where row_num<=3)
 
union all
 
(select a.*, 'deductible' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by ded_freq desc) as row_num
from default.ac_afib_Z1383_lab_cost_stat3 a) a
where row_num<=3)
 
union all
 
(select a.*, 'std_cost' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by cost_freq desc) as row_num
from default.ac_afib_Z1383_lab_cost_stat4 a) a
where row_num<=3)
 
union all
 
(select a.*, 'coins' as metric from (
select a.*, 
row_number() over (partition by a.bus, a.product order by coins_freq desc) as row_num
from default.ac_afib_Z1383_lab_cost_stat5 a) a
where row_num<=3);


Select * from ac_afib_ICD_Z1383_lab_cost_stats;

