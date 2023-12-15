-- Databricks notebook source
select distinct * from ac_multaq_afib_oop_cpt_code
order by ORGAN, code;



-- drop table if exists ac_dod_2307_member_enrol;
-- create table ac_dod_2307_member_enrol using delta location 'dbfs:/mnt/optumclin/202307/ontology/base/dod/Member Enrollment';

-- select distinct * from ac_dod_2307_member_enrol;



-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_22 as
select distinct a.*, b.*, c.bus, c.PRODUCT, c.ELIGEFF, c.ELIGEND from ac_dod_2307_med_claims a
inner join (select distinct * from ac_multaq_afib_oop_cpt_code where CODE_TYPE='CPT') b on a.proc_cd=b.CODE
left join ac_dod_2307_member_enrol c on a.PATID=c.PATID
where a.FST_DT>='2022-01-01' and a.FST_DT<='2022-12-31'
order by a.patid, a.FST_DT;

select distinct * from ac_dod_afib_lab_test_22
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from ac_dod_afib_lab_test_22
where patid='33003282106'
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_ins_type as
select distinct patid,max(eligend) as max_end_dt from ac_dod_afib_lab_test_22
group by 1
order by 1;

select distinct * from ac_dod_afib_lab_test_ins_type
order by 1;

-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_22_2 as
select distinct a.patid, bus, product from ac_dod_afib_lab_test_22 a
inner join ac_dod_afib_lab_test_ins_type b on a.patid=b.patid and a.ELIGEND=b.max_end_dt
order by a.patid;

select distinct * from ac_dod_afib_lab_test_22_2
order by patid;

-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_22_final as
select distinct a.PATID,	PAT_PLANID,	ADMIT_CHAN,	ADMIT_TYPE,	BILL_PROV,	CHARGE,	CLMID,	CLMSEQ,	COB,	COINS,	CONF_ID,	COPAY,	DEDUCT,	DRG,	DSTATUS,	ENCTR,	FST_DT,	HCCC,	ICD_FLAG,	LOC_CD,	LST_DT,	NDC,	PAID_DT,	PAID_STATUS,	POS,	PROC_CD,	PROCMOD,	PROV,	PROV_PAR,	PROVCAT,	REFER_PROV,	RVNU_CD,	SERVICE_PROV,	STD_COST,	STD_COST_YR,	TOS_CD,	UNITS,	EXTRACT_YM,	VERSION,	ALT_UNITS,	BILL_TYPE,	NDC_UOM,	NDC_QTY,	OP_VISIT_ID,	PROCMOD2,	PROCMOD3,	PROCMOD4,	TOS_EXT,	CODE,	CODE_TYPE,	ORGAN,	Description,	ELIGEFF,	ELIGEND, b.bus, b.product from ac_dod_afib_lab_test_22 a
inner join ac_dod_afib_lab_test_22_2 b on a.patid=b.patid
order by a.patid, a.FST_DT;

select distinct * from ac_dod_afib_lab_test_22_final
order by patid, fst_dt;


-- COMMAND ----------

select distinct * from ac_dod_afib_lab_test_22_final
where patid='33003282106'
order by patid, fst_dt;


-- COMMAND ----------

select distinct dx_name from ty00_all_dx_comorb

-- COMMAND ----------

create or replace table ac_dod_dx_afib_22 as
select distinct * from ac_dod_2307_med_diag
where FST_DT>='2022-01-01' and FST_DT<='2022-12-31' and DIAG like 'I48%'
order by patid, FST_DT;

select distinct * from ac_dod_dx_afib_22
order by patid, FST_DT;

-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_Liver_2022 as
select distinct a.patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost, PROC_CD, a.BUS, a.PRODUCT  from ac_dod_afib_lab_test_22_final a
where organ='Liver' and PAID_STATUS='P' 
order by a.patid, fst_dt;

select distinct * from ac_dod_afib_lab_test_Liver_2022
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from ac_dod_afib_lab_test_Liver_2022
where patid='33003285610'
order by patid, fst_dt;


-- COMMAND ----------

select distinct sum(copay) as Liver_copay, sum(deduct) as Liver_ded, sum(coins) as Liver_coins,
sum(std_cost) as Liver_std_cost from ac_dod_afib_lab_test_Liver_2022 a
inner join ac_dod_dx_afib_22 b on a.patid=b.patid ;

-- COMMAND ----------

drop table if exists ac_dod_liver_cost_max;

create table ac_dod_liver_cost_max as
select *
from (select distinct *, dense_rank() over (partition by patid, clmid, fst_dt, proc_cd order by std_cost desc, fst_dt, PROC_CD) as rank from ac_dod_afib_lab_test_Liver_2022)
-- where rank<=1
order by patid, clmid, fst_dt, proc_cd
;

select * from ac_dod_liver_cost_max;


-- COMMAND ----------

select distinct * from ac_dod_afib_lab_test_22
where COINS<0

-- COMMAND ----------

select distinct * from ac_dod_afib_lab_test_22
where patid='33004071134' and organ='Liver'
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_Lung_2022 as
select distinct patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost,bus,PRODUCT  from ac_dod_afib_lab_test_22_final 
where organ='LUNG (PFT)' and PAID_STATUS='P'
order by patid, fst_dt;

select distinct * from ac_dod_afib_lab_test_Lung_2022
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_X_Ray_2022 as
select distinct patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost, bus, PRODUCT  from ac_dod_afib_lab_test_22_final
where organ='X-ray' and PAID_STATUS='P'
order by patid, fst_dt;

select distinct * from ac_dod_afib_lab_test_X_Ray_2022
order by patid, fst_dt;

-- COMMAND ----------

select distinct * from ac_dod_afib_lab_test_22
where patid='33003282115'
order by patid, fst_dt;


-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_Thryoid_2022 as
select distinct patid, clmid, fst_dt, CHARGE,COINS, copay, deduct, std_cost, bus, PRODUCT  from ac_dod_afib_lab_test_22_final
where organ='Thyroid' and PAID_STATUS='P'
order by patid, fst_dt;

select distinct * from ac_dod_afib_lab_test_Thryoid_2022
order by patid, fst_dt;

-- COMMAND ----------

create or replace table ac_dod_afib_lab_test_PT_2022 as
select distinct patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost, bus, PRODUCT  from ac_dod_afib_lab_test_22_final
where organ='PT/INR' and PAID_STATUS='P' 
order by patid, fst_dt;

select distinct * from ac_dod_afib_lab_test_PT_2022
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_dod_afib_lab_test_Eye_2022 as
select distinct patid, clmid, fst_dt, charge, COINS, copay, deduct, std_cost, BUS, PRODUCT  from ac_dod_afib_lab_test_22_final
where organ='Eye' and PAID_STATUS='P'
order by patid, fst_dt;

select distinct * from ac_dod_afib_lab_test_Eye_2022
order by patid, fst_dt;

-- COMMAND ----------

-- drop table ac_dod_dx_afib_lab_test_cost;
-- select distinct a.patid, sum(Liver_copay) as Liver_copay, sum(Liver_ded) as Liver_ded, sum(Liver_coins) as Liver_coins, sum(Liver_std_cost) as Liver_std_cost, max(Lung_copay) as Lung_copay, max(Lung_ded) as Lung_ded, max(Lung_coins) as Lung_coins, max(Lung_std_cost) as Lung_std_cost,
--  max(X_Ray_copay) as X_Ray_copay, max(X_Ray_ded) as X_Ray_ded, max(X_Ray_coins) as X_Ray_coins, max(X_Ray_std_cost) as X_Ray_std_cost,
-- max(PT_copay) as PT_copay, max(PT_ded) as PT_ded, max(PT_coins) as PT_coins, max(PT_std_cost) as PT_std_cost,
-- max(Eye_copay) as Eye_copay, max(Eye_ded) as Eye_ded, max(Eye_coins) as Eye_coins, max(Eye_std_cost) as Eye_std_cost

-- from (select distinct patid from ac_dod_dx_afib_22) a
-- left join (select distinct patid, sum(copay) as Liver_copay, sum(deduct) as Liver_ded, sum(coins) as Liver_coins,
-- sum(std_cost) as Liver_std_cost from ac_dod_afib_lab_test_Liver_2022 group by patid) b on a.patid=b.patid

-- left join (select distinct patid, sum(copay) as Lung_copay, sum(deduct) as Lung_ded, sum(coins) as Lung_coins,
-- sum(std_cost) as Lung_std_cost from ac_dod_afib_lab_test_Lung_2022 group by patid) c on a.patid=c.patid

-- left join (select distinct patid, sum(copay) as X_Ray_copay, sum(deduct) as X_Ray_ded, sum(coins) as X_Ray_coins,
-- sum(std_cost) as X_Ray_std_cost from ac_dod_afib_lab_test_X_Ray_2022 group by patid) d on a.patid=d.patid

-- left join (select distinct patid, sum(copay) as PT_copay, sum(deduct) as PT_ded, sum(coins) as PT_coins,
-- sum(std_cost) as PT_std_cost from ac_dod_afib_lab_test_PT_2022 group by patid) e on a.patid=e.patid

-- left join (select distinct patid, sum(copay) as Eye_copay, sum(deduct) as Eye_ded, sum(coins) as Eye_coins,
-- sum(std_cost) as Eye_std_cost from ac_dod_afib_lab_test_Eye_2022 group by patid) f on a.patid=f.patid
-- group by a.patid
-- order by a.patid;

-- select distinct * from ac_dod_dx_afib_lab_test_cost
-- order by patid;

-- COMMAND ----------


create or replace table ac_afib_liver_lab_cost_1 as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as Liver_ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_afib_lab_test_Liver_2022
where patid in (select distinct patid from ac_dod_dx_afib_22)
group by patid,2,3
order by patid,2,3;

select distinct * from ac_afib_liver_lab_cost_1
order by patid;

-- select sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_liver_lab_cost

-- COMMAND ----------

 select bus, product, sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_liver_lab_cost_1
 group by 1,2
 order by 1,2;



-- COMMAND ----------

select distinct * from ac_afib_liver_lab_cost
where patid='33003285610'


-- COMMAND ----------

ac_dod_afib_lab_test_Liver_2022

-- COMMAND ----------


create or replace table ac_no_afib_liver_lab_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_afib_lab_test_Liver_2022 a
where patid not in (select distinct patid from ac_afib_liver_lab_cost)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_no_afib_liver_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_no_afib_liver_lab_cost

-- COMMAND ----------


create or replace table ac_afib_lung_lab_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc,  sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_afib_lab_test_Lung_2022
where patid in (select distinct patid from ac_dod_dx_afib_22)  
group by patid, 2, 3
order by 1,2,3;

select distinct * from ac_afib_lung_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_lung_lab_cost;


-- COMMAND ----------


create or replace table ac_no_afib_lung_lab_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_afib_lab_test_Lung_2022
where patid not in (select distinct patid from ac_afib_lung_lab_cost)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_no_afib_lung_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_no_afib_lung_lab_cost

-- COMMAND ----------


create or replace table ac_afib_x_ray_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_afib_lab_test_X_Ray_2022
where patid in (select distinct patid from ac_dod_dx_afib_22)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_afib_x_ray_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_x_ray_lab_cost;


-- COMMAND ----------


create or replace table ac_no_afib_x_ray_lab_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_afib_lab_test_X_Ray_2022
where patid not in (select distinct patid from ac_afib_x_ray_lab_cost)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_no_afib_x_ray_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_no_afib_lung_lab_cost

-- COMMAND ----------


create or replace table ac_afib_thyroid_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_afib_lab_test_Thryoid_2022
where patid in (select distinct patid from ac_dod_dx_afib_22)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_afib_thyroid_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_thyroid_lab_cost;


-- COMMAND ----------


create or replace table ac_no_afib_thyroid_lab_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_afib_lab_test_Thryoid_2022
where patid not in (select distinct patid from ac_afib_thyroid_lab_cost)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_no_afib_thyroid_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_no_afib_lung_lab_cost

-- COMMAND ----------


create or replace table ac_afib_PT_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_afib_lab_test_PT_2022
where patid in (select distinct patid from ac_dod_dx_afib_22)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_afib_PT_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_PT_lab_cost;


-- COMMAND ----------


create or replace table ac_no_afib_PT_lab_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_afib_lab_test_PT_2022
where patid not in (select distinct patid from ac_afib_PT_lab_cost)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_no_afib_PT_lab_cost
order by patid;

-- COMMAND ----------


create or replace table ac_afib_eye_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_afib_lab_test_Eye_2022
where patid in (select distinct patid from ac_dod_dx_afib_22)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_afib_eye_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_eye_lab_cost;


-- COMMAND ----------


create or replace table ac_no_afib_eye_lab_cost as
select distinct patid,bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins, sum(std_cost) as std_cost from ac_dod_afib_lab_test_Eye_2022
where patid not in (select distinct patid from ac_afib_eye_lab_cost)
group by patid,2,3
order by 1,2,3;

select distinct * from ac_no_afib_eye_lab_cost
order by patid;

-- COMMAND ----------

select ' 01 Liver' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(liver_ded) as liver_ded, sum(std_cost) as std_cost  from ac_afib_liver_lab_cost_1
group by bus, product
order by bus, product;
select ' 02 Lung' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost   from ac_afib_lung_lab_cost
group by bus, product
order by bus, product;

select ' 03 X-Ray' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_afib_x_ray_lab_cost
group by bus, product
order by bus, product;

select ' 04 Thyroid' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_afib_thyroid_lab_cost
group by bus, product
order by bus, product;

select ' 05 PT' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_afib_PT_lab_cost
group by bus, product
order by bus, product;

select ' 06 Eye' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_afib_eye_lab_cost
group by bus, product
order by bus, product;

-- COMMAND ----------

select ' 01 Liver' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost  from ac_no_afib_liver_lab_cost
group by bus, product
order by bus, product;
select ' 02 Lung' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost   from ac_no_afib_lung_lab_cost
group by bus, product
order by bus, product;

select ' 03 X-Ray' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_no_afib_x_ray_lab_cost
group by bus, product
order by bus, product;

select ' 04 Thyroid' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_no_afib_thyroid_lab_cost
group by bus, product
order by bus, product;

select ' 05 PT' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_no_afib_PT_lab_cost
group by bus, product
order by bus, product;

select ' 06 Eye' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_no_afib_eye_lab_cost
group by bus, product
order by bus, product;

-- COMMAND ----------

create or replace table ac_dod_afib_lab_dx_test_22 as
select distinct a.PATID,a.clmid, a.diag_position, a.diag, a.POA, a.FST_DT, c.POS, c.charge, c.COPAY, c.DEDUCT, c.COINS,c.STD_COST,b.* from ac_dod_2307_med_diag a
inner join (select distinct * from ac_multaq_afib_oop_cpt_code where CODE_TYPE='ICD10') b on a.DIAG=b.CODE
inner join ac_dod_2307_med_claims c on a.PATID=c.patid and a.CLMID=c.CLMID and a.FST_DT=c.FST_DT
where a.FST_DT>='2022-01-01' and a.FST_DT<='2022-12-31' and c.PAID_STATUS='P'
order by a.PATID, a.FST_DT;

select distinct * from ac_dod_afib_lab_dx_test_22
order by PATID, fst_dt; 

-- COMMAND ----------

create or replace table ac_dod_afib_lab_dx_test_22_ins as
select distinct a.patid, b.bus, b.product, b.ELIGEFF, b.ELIGEND from ac_dod_afib_lab_dx_test_22 a
left join ac_dod_2307_member_enrol b on a.PATID=b.PATID
order by a.patid;

select distinct * from ac_dod_afib_lab_dx_test_22_ins
order by patid;

-- COMMAND ----------

create or replace table ac_dod_afib_dx_test_max_ins_type as
select distinct patid,max(eligend) as max_end_dt from ac_dod_afib_lab_dx_test_22_ins
group by 1
order by 1;

select distinct * from ac_dod_afib_dx_test_max_ins_type
order by 1;

-- COMMAND ----------


create or replace table ac_dod_afib_lab_dx_test_22_ins_2 as
select distinct a.patid, bus, product from ac_dod_afib_lab_dx_test_22_ins a
inner join ac_dod_afib_dx_test_max_ins_type b on a.patid=b.patid and a.ELIGEND=b.max_end_dt
order by a.patid;

select distinct * from ac_dod_afib_lab_dx_test_22_ins_2
order by patid;

-- COMMAND ----------

create or replace table ac_dod_afib_dx_test_22_final as
select distinct a.*, b.bus, b.product from ac_dod_afib_lab_dx_test_22 a
inner join ac_dod_afib_lab_dx_test_22_ins_2 b on a.patid=b.patid
order by a.PATID, a.FST_DT;

select distinct * from ac_dod_afib_dx_test_22_final
order by patid,fst_dt;

-- COMMAND ----------

create or replace table ac_dod_dx_lab_test_Z79899 as
select distinct patid, clmid, bus, product, fst_dt, diag , charge, copay, deduct, COINS, STD_COST from ac_dod_afib_dx_test_22_final
where DIAG='Z79899' and diag_position in ('01','02')
order by patid, FST_DT;

select distinct * from ac_dod_dx_lab_test_Z79899
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_afib_Z79899_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z79899 a
where patid in (select distinct patid from ac_dod_dx_afib_22 ) 
group by 1,2,3
order by 1,2,3;

select distinct * from ac_afib_Z79899_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_Z79899_lab_cost;


-- COMMAND ----------

create or replace table ac_no_afib_Z79899_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z79899
where patid not in (select distinct patid from ac_afib_Z79899_lab_cost)
group by 1,2,3
order by 1,2,3;

select distinct * from ac_no_afib_Z79899_lab_cost
order by 1,2,3;


-- COMMAND ----------

create or replace table ac_dod_dx_lab_test_Z0000 as
select distinct patid, clmid, fst_dt,bus, product, diag , charge, copay, deduct, COINS, STD_COST from ac_dod_afib_dx_test_22_final
where DIAG='Z0000' and diag_position in ('01','02')
order by patid, FST_DT;

select distinct * from ac_dod_dx_lab_test_Z0000
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_afib_Z0000_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z0000 a
where patid in (select distinct patid from ac_dod_dx_afib_22)  
group by patid,2,3
order by 1,2,3;

select distinct * from ac_afib_Z0000_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_Z0000_lab_cost;


-- COMMAND ----------

create or replace table ac_no_afib_Z0000_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z0000
where patid not in (select distinct patid from ac_afib_Z0000_lab_cost)
group by 1,2,3
order by 1,2,3;

select distinct * from ac_no_afib_Z0000_lab_cost
order by 1,2,3;


-- COMMAND ----------


create or replace table ac_dod_dx_lab_test_Z139 as
select distinct patid, clmid,bus,product, fst_dt, diag , charge, copay, deduct, COINS, STD_COST from ac_dod_afib_dx_test_22_final
where DIAG='Z139' and diag_position in ('01','02')
order by patid, FST_DT;

select distinct * from ac_dod_dx_lab_test_Z139
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_afib_Z139_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z139
where patid in (select distinct patid from ac_dod_dx_afib_22)  
group by patid, bus, product;

select distinct * from ac_afib_Z139_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_Z139_lab_cost;


-- COMMAND ----------

create or replace table ac_no_afib_Z139_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z139
where patid not in (select distinct patid from ac_afib_Z139_lab_cost)
group by 1,2,3
order by 1,2,3;

select distinct * from ac_no_afib_Z139_lab_cost
order by 1,2,3;


-- COMMAND ----------




create or replace table ac_dod_dx_lab_test_Z0100 as
select distinct patid, clmid, bus, product, fst_dt, diag , charge, copay, deduct, COINS, STD_COST from ac_dod_afib_dx_test_22_final
where DIAG='Z0100' and diag_position in ('01','02')
order by patid, FST_DT;

select distinct * from ac_dod_dx_lab_test_Z0100
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_afib_Z0100_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z0100 
where patid in (select distinct patid from ac_dod_dx_afib_22) 
group by patid, bus, product
order by 1,2,3;

select distinct * from ac_afib_Z0100_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_Z0100_lab_cost;


-- COMMAND ----------

create or replace table ac_no_afib_Z0100_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z0100
where patid not in (select distinct patid from ac_afib_Z0100_lab_cost)
group by 1,2,3
order by 1,2,3;

select distinct * from ac_no_afib_Z0100_lab_cost
order by 1,2,3;


-- COMMAND ----------

create or replace table ac_dod_dx_lab_test_Z1383 as
select distinct patid, clmid,bus, product, fst_dt, diag ,charge, copay, deduct, COINS, STD_COST from ac_dod_afib_dx_test_22_final
where DIAG='Z1383' and diag_position in ('01','02')
order by patid, FST_DT;

select distinct * from ac_dod_dx_lab_test_Z1383
order by patid, fst_dt;

-- COMMAND ----------


create or replace table ac_afib_Z1383_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z1383
where patid in (select distinct patid from ac_dod_dx_afib_22) 
group by patid, bus, product
order by patid, bus, product;

select distinct * from ac_afib_Z1383_lab_cost
order by patid;

-- select count(distinct patid), sum(liver_copay), sum(liver_ded), sum(liver_coins), sum(liver_std_cost) from ac_afib_Z1383_lab_cost;


-- COMMAND ----------

create or replace table ac_no_afib_Z1383_lab_cost as
select distinct patid, bus, product, count(distinct fst_dt) as n_proc, sum(charge) as charge, sum(copay) as copay, sum(deduct) as ded, sum(coins) as coins,
sum(std_cost) as std_cost from ac_dod_dx_lab_test_Z1383
where patid not in (select distinct patid from ac_afib_Z1383_lab_cost)
group by 1,2,3
order by 1,2,3;

select distinct * from ac_no_afib_Z1383_lab_cost
order by 1,2,3;


-- COMMAND ----------

select ' 01 Z79899' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost  from ac_afib_Z79899_lab_cost
group by bus, product
order by bus, product;
select ' 02 Z0000' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost   from ac_afib_Z0000_lab_cost
group by bus, product
order by bus, product;

select ' 03 Z139' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_afib_Z139_lab_cost
group by bus, product
order by bus, product;

select ' 04 Z0100' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_afib_Z0100_lab_cost
group by bus, product
order by bus, product;

select ' 05 Z1383' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_afib_Z1383_lab_cost
group by bus, product
order by bus, product;

-- COMMAND ----------

select ' 01 Z79899' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost  from ac_no_afib_Z79899_lab_cost
group by bus, product
order by bus, product;
select ' 02 Z0000' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as ded, sum(std_cost) as std_cost   from ac_no_afib_Z0000_lab_cost
group by bus, product
order by bus, product;

select ' 03 Z139' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_no_afib_Z139_lab_cost
group by bus, product
order by bus, product;

select ' 04 Z0100' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_no_afib_Z0100_lab_cost
group by bus, product
order by bus, product;

select ' 05 Z1383' as staus, bus, product, count(distinct patid) as n_pts, sum(n_proc) as procs, sum(charge) as charge, 
sum(copay) as copay, sum(coins) as coins, sum(ded) as deduct, sum(std_cost) as std_cost   from ac_no_afib_Z1383_lab_cost
group by bus, product
order by bus, product;

-- COMMAND ----------


