-- Databricks notebook source
drop table if exists ac_dod_2304_lu_NDC;

create table ac_dod_2304_lu_NDC using delta location 'dbfs:/mnt/optumclin/202304/ontology/base/dod/Lookup NDC';

select * from ac_dod_2304_lu_NDC;


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

select count(*) from ac_dod_2304_lu_NDC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("ac_dod_2304_lu_NDC").write.format("csv").mode("overwrite").option("header","true").save("dbfs:/mnt/rwe-projects-ac/amit");
