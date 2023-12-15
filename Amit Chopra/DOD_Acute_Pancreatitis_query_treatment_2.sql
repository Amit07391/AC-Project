-- Databricks notebook source
-- MAGIC %md #### Checking treatment for acute pancreatitis within 30 days of the index date

-- COMMAND ----------

-- create or replace table ac_dod_act_pancr_trment_1 as
-- select distinct a.*, b.yr_dt, b.indx_dt
-- from ac_dod_2303_RX_claims a
-- inner join ac_cdm_dx_ac_pancrtis_indx_1 b on a.patid=b.patid and year(a.fill_dt)=b.yr_dt
-- where a.fill_dt between b.indx_dt and b.indx_dt + 30
-- order by a.patid, a.fill_dt;

select distinct * from ac_dod_act_pancr_trment_1
order by patid, fill_dt;

-- COMMAND ----------

select distinct b.ahfsclss_desc, a.ndc, a.brnd_nm, a.gnrc_nm from ac_dod_act_pancr_trment_1 a
inner join ac_dod_2303_lu_NDC b on a.ndc=b.ndc


-- COMMAND ----------

create or replace table ac_dod_act_pancr_trment_2 as
select distinct * from ac_dod_act_pancr_trment_1
where upper(brnd_nm) in ('TRAMADOL HCL ER',
'AMPICILLIN TRIHYDRATE',
'ULTRAM',
'CEFTRIAXONE',
'TRAMADOL HCL',
'MEPERIDINE HCL',
'HYDROCODONE-ACETAMINOPHEN',
'TRAMADOL HCL-ACETAMINOPHEN',
'ACETAMINOPHEN-CODEINE',
'BUTALBITAL-ACETAMINOPHEN-CAFFE',
'OXYCODONE-ACETAMINOPHEN',
'AMPICILLIN-SULBACTAM',
'IMIPENEM-CILASTATIN SODIUM',
'AMPICILLIN SODIUM',
'BUTALBITAL-ACETAMINOPHEN',
'CONZIP',
'ACETAMINOPHEN-BUTALBITAL',
'ACETAMINOPHEN',
'CEFTRIAXONE SODIUM',
'HYDROCODONE/ACETAMINOPHEN',
'TRAMADOL HCL/ACETAMINOPHEN',
'ACETAMINOPHEN WITH CODEINE',
'BUTALB/ACETAMINOPHEN/CAFFEINE',
'OXYCODONE HCL/ACETAMINOPHEN',
'AMPICILLIN SODIUM/SULBACTAM NA',
'CEFTRIAXONE NA/DEXTROSE,ISO',
'IMIPENEM/CILASTATIN SODIUM',
'AMPICILLIN SOD/SULBACTAM SOD',
'CEFTRIAXONE IN IS-OSM DEXTROSE',
'BUTALBITAL/ACETAMINOPHEN',
'ACETAMINOPHEN/CAFF/DIHYDROCOD'
)
or upper(gnrc_nm) in ('TRAMADOL HCL ER',
'AMPICILLIN TRIHYDRATE',
'ULTRAM',
'CEFTRIAXONE',
'TRAMADOL HCL',
'MEPERIDINE HCL',
'HYDROCODONE-ACETAMINOPHEN',
'TRAMADOL HCL-ACETAMINOPHEN',
'ACETAMINOPHEN-CODEINE',
'BUTALBITAL-ACETAMINOPHEN-CAFFE',
'OXYCODONE-ACETAMINOPHEN',
'AMPICILLIN-SULBACTAM',
'IMIPENEM-CILASTATIN SODIUM',
'AMPICILLIN SODIUM',
'BUTALBITAL-ACETAMINOPHEN',
'CONZIP',
'ACETAMINOPHEN-BUTALBITAL',
'ACETAMINOPHEN',
'CEFTRIAXONE SODIUM',
'HYDROCODONE/ACETAMINOPHEN',
'TRAMADOL HCL/ACETAMINOPHEN',
'ACETAMINOPHEN WITH CODEINE',
'BUTALB/ACETAMINOPHEN/CAFFEINE',
'OXYCODONE HCL/ACETAMINOPHEN',
'AMPICILLIN SODIUM/SULBACTAM NA',
'CEFTRIAXONE NA/DEXTROSE,ISO',
'IMIPENEM/CILASTATIN SODIUM',
'AMPICILLIN SOD/SULBACTAM SOD',
'CEFTRIAXONE IN IS-OSM DEXTROSE',
'BUTALBITAL/ACETAMINOPHEN',
'ACETAMINOPHEN/CAFF/DIHYDROCOD'
)
order by patid, fill_dt;

select distinct * from ac_dod_act_pancr_trment_2
order by patid, fill_dt;

-- COMMAND ----------

-- MAGIC %md #### Checking procedure for diagnosed patients

-- COMMAND ----------

create or replace table ac_dod_act_panc_proc as
select distinct a.*, b.yr_dt, b.indx_dt, c.proc_desc from ac_dod_2303_med_proc a
inner join ac_cdm_dx_ac_pancrtis_indx_1 b on a.patid=b.patid and year(a.fst_dt)=b.yr_dt
left join ac_dod_2303_lu_proc c on a.proc=c.proc_cd
where a.fst_dt between b.indx_dt and b.indx_dt + 30
order by a.patid, a.fst_dt;

select distinct * from ac_dod_act_panc_proc;

-- COMMAND ----------

select distinct proc_desc,proc, count(distinct patid) from ac_dod_act_panc_proc
group by 1,2
order by 3 desc

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_med_clm_proc as
select distinct a.patid, a.clmid, a.pos, a.proc_cd, a.fst_dt, b.yr_dt, b.indx_dt, c.proc_desc from ac_dod_2303_med_claims a
inner join ac_cdm_dx_ac_pancrtis_indx_1 b on a.patid=b.patid and year(a.fst_dt)=b.yr_dt
left join ac_dod_2303_lu_proc c on a.proc_cd=c.proc_cd
where a.fst_dt between b.indx_dt and b.indx_dt + 30
order by a.patid, a.fst_dt;

select distinct * from ac_dod_dx_act_pancrtis_med_clm_proc;

-- COMMAND ----------

create or replace table ac_dod_dx_act_pancrtis_med_clm_proc_1 as
select distinct * from ac_dod_dx_act_pancrtis_med_clm_proc
where proc_cd in ('47480',
'47564',
'47612',
'48001',
'47563',
'47610',
'48000',
'47562',
'47620',
'47605',
'47600',
'47490'
);

select distinct * from ac_dod_dx_act_pancrtis_med_clm_proc_1
order by patid, fst_dt;

-- COMMAND ----------

select distinct proc_desc, proc_cd, count(distinct patid) from ac_dod_dx_act_pancrtis_med_clm_proc
group by 1,2
order by 3 desc

-- COMMAND ----------

-- MAGIC %md #### Combining procedure and med proc 

-- COMMAND ----------

create or replace table ac_dod_act_pancrt_med_proc_001 as
select distinct patid, yr_dt from ac_dod_act_panc_proc
where proc in ('0FT44ZZ','0FT40ZZ')
union
select distinct patid, yr_dt from ac_dod_dx_act_pancrtis_med_clm_proc_1;


select distinct yr_dt, count(distinct patid) from ac_dod_act_pancrt_med_proc_001
group by 1
order by 1

-- COMMAND ----------

create or replace table ac_dod_act_pacrt_proc_nms as 
select distinct proc_cd, proc_desc from ac_dod_dx_act_pancrtis_med_clm_proc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC
-- MAGIC from pyspark.sql.functions import expr
-- MAGIC from pyspark.sql.functions import lit, when, concat, trim, col, desc, to_date, datediff, year,to_csv,date_add
-- MAGIC from pyspark.sql.functions import countDistinct
-- MAGIC
-- MAGIC from delta.tables import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("ac_dod_act_pacrt_proc_nms").write.format("csv").mode("overwrite").option("header","true").save("dbfs:/mnt/rwe-projects-ac/amit");
