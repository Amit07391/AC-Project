-- Databricks notebook source
create or replace table ac_dod_t1d_antibody_lab_1 as
select distinct * from ac_dod_2301_lab_claim
where loinc_cd in ('56718-0',
'56546-5',
'56540-8',
'13927-9',
'76651-9');

select distinct * from ac_dod_t1d_antibody_lab_1
order by patid, fst_dt;

-- COMMAND ----------

select count (*) from ac_dod_t1d_antibody_lab_1

-- COMMAND ----------

select distinct rslt_nbr, rslt_txt, rslt_unit_nm from ac_dod_t1d_antibody_lab_1
where loinc_cd='13927-9' 

-- COMMAND ----------

select distinct rslt_nbr from ac_dod_t1d_antibody_lab_1

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_2 as
select distinct *, case when rslt_unit_nm='IU/L' then cast(rslt_nbr as float)/1000 
else cast(rslt_nbr as float) end as Result_value 
from ac_dod_t1d_antibody_lab_1
-- where rslt_unit_nm not in ('null',
-- 'JDF UNITS',
-- 'JDF Units',
-- 'N',
-- '{titer}',
-- '%',
-- 'ML',
-- 'JDF units',
-- 'nmol/L',
-- 'NULL',
-- 'units',
-- 'JDF_UNITS')
order by patid, fst_dt;


-- COMMAND ----------

select distinct * from ac_dod_t1d_antibody_lab_2


-- COMMAND ----------

-- MAGIC %md #### Checking only max test value

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_max_value as
select distinct patid, loinc_cd, max(result_value) as Max_value from ac_dod_t1d_antibody_lab_2
group by 1,2
order by 1,2;

select distinct * from ac_dod_t1d_antibody_lab_max_value;

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_3 as
select distinct a.*, b.max_value from ac_dod_t1d_antibody_lab_2 a
inner join ac_dod_t1d_antibody_lab_max_value b on a.patid=b.patid and a.loinc_cd=b.loinc_cd
order by a.patid, fst_dt;

select distinct * from ac_dod_t1d_antibody_lab_3
where patid='33005784080';



-- COMMAND ----------

select distinct loinc_cd, rslt_nbr, rslt_txt, rslt_unit_nm from ac_dod_t1d_antibody_lab_3
where rslt_nbr is null or rslt_nbr=0;

-- COMMAND ----------

-- MAGIC %md #### Using both max and any test value

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_4 as
select distinct *,
case when loinc_cd='56718-0' and (result_value>=5.4 or rslt_txt in ('>350.0','>350.0','>221.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='56546-5' and result_value>=0.4 and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='56540-8' and (result_value>=5 or rslt_txt in ('>126.0',
'>250.0',
'>134.0',
'>250',
'>119.0',
'>154.0',
'>120',
'>250',
'>132.0',
'>121.0',
'>250.0',
'>120',
'>105.0',
'>114.7',
'>146.0',
'>119.0',
'>120',
'>108.0',
'>142.0',
'>142.0',
'>129.0',
'>147.0',
'>128.0',
'>148.9',
'>250.0',
'>183.0',
'>116.0',
'>115.0',
'>118.0',
'>141.0',
'>146.0',
'>114.7',
'>174.0',
'>159.0',
'>160.0',
'>181.0',
'>163.0',
'>157.0',
'>162.0',
'>148.0',
'>168.0',
'>144.0',
'>149.0',
'>172.0',
'>130.0',
'>149.8',
'>113.0',
'>161.0',
'>104.0',
'>139.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='76651-9' and (result_value>=15 or rslt_txt in ('>500.0',
'>500')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'

end as Flag_Result,

case when loinc_cd='56718-0' and (max_value>=5.4 or rslt_txt in ('>350.0','>350.0','>221.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS')  then 'Positive'
when loinc_cd='56546-5' and max_value>=0.4 and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS')  then 'Positive'
when loinc_cd='56540-8' and (max_value>=5 or rslt_txt in ('>126.0',
'>250.0',
'>134.0',
'>250',
'>119.0',
'>154.0',
'>120',
'>250',
'>132.0',
'>121.0',
'>250.0',
'>114.7',
'>120',
'>105.0',
'>146.0',
'>119.0',
'>120',
'>108.0',
'>142.0',
'>142.0',
'>129.0',
'>147.0',
'>128.0',
'>148.9',
'>250.0',
'>183.0',
'>116.0',
'>115.0',
'>118.0',
'>141.0',
'>146.0',
'>114.7',
'>174.0',
'>159.0',
'>160.0',
'>181.0',
'>163.0',
'>157.0',
'>162.0',
'>148.0',
'>168.0',
'>144.0',
'>149.0',
'>172.0',
'>130.0',
'>149.8',
'>113.0',
'>161.0',
'>104.0',
'>139.0')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS') then 'Positive'
when loinc_cd='76651-9' and ( max_value>=15 or rslt_txt in ('>500.0',
'>500')) and rslt_unit_nm not in ('null',
'JDF UNITS',
'JDF Units',
'N',
'{titer}',
'%',
'ML',
'JDF units',
'nmol/L',
'NULL',
'units',
'JDF_UNITS')  then 'Positive' end as Flag_Result_max_value
from ac_dod_t1d_antibody_lab_3
order by patid, fst_dt;

select distinct * from ac_dod_t1d_antibody_lab_4
order by patid, fst_dt;

-- COMMAND ----------

select loinc_cd, count(distinct patid) as cnts from ac_dod_t1d_antibody_lab_4
where Flag_Result='Positive'
group by 1;

-- COMMAND ----------

-- MAGIC %md ##### Working on one LOINC 13927-9

-- COMMAND ----------

create or replace table ac_dod_t1d_antibdy_lab_loinc_13927 as 
select distinct *,
case when loinc_cd='13927-9' and rslt_txt = '1:02' then 2*5
when loinc_cd='13927-9' and rslt_txt = '1:16' then 16*5
when loinc_cd='13927-9' and rslt_txt = '1:08' then 8*5
when loinc_cd='13927-9' and rslt_txt = '1:32' then 32*5
when loinc_cd='13927-9' and rslt_txt = '1:04' then 4*5
else result_value end as Loinc_value

from ac_dod_t1d_antibody_lab_3
where loinc_cd='13927-9';

select distinct * from ac_dod_t1d_antibdy_lab_loinc_13927
order by patid, fst_dt;




-- COMMAND ----------

select count(distinct patid) as cnts from ac_dod_t1d_antibdy_lab_loinc_13927
where loinc_value>=10

-- COMMAND ----------

-- MAGIC %md ####Other analysis ( use till here)

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
-- MAGIC spark.table("ac_dod_t1d_antibody_lab_1").write.format("csv").mode("overwrite").option("header","true").save("dbfs:/mnt/rwe-projects-ac/amit/T1D_antibody");

-- COMMAND ----------

select distinct proc_cd,tst_desc from ac_dod_t1d_antibody_lab_1

-- COMMAND ----------

create or replace table ac_dod_t1d_antibody_lab_proc_1 as
select distinct * from ac_dod_2301_lab_claim
where proc_cd in ('86341','86337');

select distinct * from ac_dod_t1d_antibody_lab_proc_1
order by patid, fst_dt;

-- COMMAND ----------


select distinct loinc_cd, tst_desc from ac_dod_2301_lab_claim
where loinc_cd in ('56718-0',
'94344-9',
'94288-8',
'45225-0',
'72166-2',
'5265-4',
'82976-2',
'1975-2',
'33563-8',
'94352-2',
'31209-0',
'56540-8',
'2093-3',
'77202-0',
'UNLOINC',
'69048-7',
'94347-2',
'94355-5',
'null',
'93503-1',
'9279-1',
'93428-1',
'2075-0',
'19146-0',
'82989-5',
'30347-9',
'742-7',
'13927-9',
'8086-1',
'788-0',
'2028-9',
'94345-6',
'94342-3',
'56687-7',
'SOLOINC',
'3024-7',
'0000-0',
'56546-5',
'31547-3',
'27038-9',
'1920-8',
'94815-8',
'2481-0',
'3695-4',
'94285-4',
'76651-9',
'94343-1',
'29463-7',
'94346-4',
'13926-1',
'785-6',
'2571-8',
'93502-3',
'8074-7',
'94351-4',
'160721',
'94350-6',
'94340-7',
'8072-1',
'8462-4',
'PENDING',
'1986-9',
'94287-0',
'770-8',
'49549-9',
'43942-2',
'8251-1',
'20448-7',
'8265-1',
'5232-4',
'93489-3',
'13458-5',
'17861-6',
'68535-4',
'2951-2',
'94360-5',
'4548-4',
'2482-8',
'8867-4',
'6873-4',
'96490-8',
'42501-7',
'2345-7',
'94357-1',
'99999',
'2484-4',
'68994-3',
'48642-3',
'94676-4',
'3094-0',
'3091-6',
'50138',
'789-8',
'73830-2',
'61151-7',
'718-7',
'10451-3',
'1742-6',
'94356-3',
'1963-8',
'24323-8',
'8302-2',
'93426-5',
'2284-8',
'3140-1',
'39156-5',
'31017-7',
'8310-5',
'786-4',
'96486-6',
'93491-9',
'4544-3',
'60463-7',
'94359-7',
'1759-0',
'713-8',
'1744-2',
'94341-5',
'2085-9',
'27882-0',
'34543-9',
'73832-8',
'96479-1',
'29257-3',
'2161-8',
'8480-6',
'777-3',
'10362-2',
'94364-7',
'664-3',
'94354-8',
'2339-0',
'2132-9',
'6690-2',
'33256-9',
'96476-7',
'2160-0',
'704-7',
'10834-0',
'2881-1',
'99228',
'6768-6',
'48643-1',
'94358-9',
'5905-5',
'94362-1',
'94706-9',
'736-9',
'59408-5',
'2885-2',
'94361-3',
'3097-3',
'787-2',
'32623-1',
'2823-3',
'32998-7',
'6301-6',
'94286-2',
'14957-5',
'19123-9',
'58709-7',
'9318-7',
'62238-1',
'73561-3',
'46099-8',
'731-0',
'19327-9',
'13457-7',
'706-2',
'711-2',
'33037-3',
'18182-6',
'94363-9',
'5902-2',
'27353-2',
'2886-0',
'11580-8',
'751-8',
'17856-6',
'1751-7',
'32636-3',
'58710-5');


-- COMMAND ----------

create table ac_dod_t1d_islet_lab_loinc_1 as
select distinct * from ac_dod_2301_lab_claim
where loinc_cd in (
'5265-4',
'31209-0',
'8086-1',
'31547-3',
'13926-1',
'42501-7',
'32636-3'
);

select count(*) from ac_dod_t1d_islet_lab_loinc_1;

-- COMMAND ----------

select distinct * from ac_dod_t1d_islet_lab_loinc_1
order by patid, fst_dt;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("ac_dod_t1d_islet_lab_loinc_1").write.format("csv").mode("overwrite").option("header","true").save("dbfs:/mnt/rwe-projects-ac/amit/T1D_antibody/islet_loinc");

-- COMMAND ----------

create or replace table ac_dod_t1d_dysgly_lab_proc_1 as
select distinct patid, fst_dt, hi_nrml, labclmid, loinc_cd, low_nrml, proc_cd, rslt_nbr, RSLT_TXT, RSLT_UNIT_NM, TST_DESC from ac_dod_2301_lab_claim
where proc_cd in ('82951','82947','82950','83036') ;
-- and upper(tst_desc) not in ('',' ','UNK','NONE','UNKNOWN','NOCPT');

select distinct * from ac_dod_t1d_dysgly_lab_proc_1
order by patid, fst_dt;

-- COMMAND ----------

select count(*) from ac_dod_t1d_dysgly_lab_proc_1
