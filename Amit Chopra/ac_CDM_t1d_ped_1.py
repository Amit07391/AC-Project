# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC from pyspark.sql.types import StringType
# MAGIC from pyspark.sql.types import IntegerType
# MAGIC
# MAGIC from pyspark.sql.functions import expr
# MAGIC from pyspark.sql.functions import lit, when, concat, trim, col, desc, to_date, datediff, year,to_csv,date_add
# MAGIC from pyspark.sql.functions import countDistinct
# MAGIC
# MAGIC from delta.tables import *

# COMMAND ----------


