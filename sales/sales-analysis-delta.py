# Databricks notebook source
from delta.tables import *
import pyspark.sql.functions as sf

# COMMAND ----------

dbutils.widgets.text("p_load_type","")
dbutils.widgets.text("p_load_date","")

v_load_type = dbutils.widgets.get("p_load_type")
v_load_date = dbutils.widgets.get("p_load_date")

# COMMAND ----------

if v_load_type == "initial":
    spark.sql("drop database if exists sales_delta_db cascade")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists sales_delta_db
# MAGIC location '/mnt/lakehouse/gold/sales_delta_db'

# COMMAND ----------

# MAGIC %sql
# MAGIC use sales_delta_db

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists sales_delta_db.country_wise_daily_sales (
# MAGIC   country string,
# MAGIC   invoice_year int,
# MAGIC   invoice_month int,
# MAGIC   invoice_date date,
# MAGIC   total_sales string
# MAGIC
# MAGIC ) using delta
# MAGIC partitioned by (country,invoice_year,invoice_month)

# COMMAND ----------

def full_delta_load(file_path):
    return DeltaTable.forPath(spark,file_path).toDF()

# COMMAND ----------

def full_analysis():
    spark.sql(f"""insert overwrite table  sales_delta_db.country_wise_daily_sales
              select country, invoice_year,invoice_month,invoice_date, round(sum(quantity*unit_price),2)
              from invoice_tbl_silver group by country, invoice_year,invoice_month,invoice_date
               """)

# COMMAND ----------

def load_incremental_source(location):
    full_source = full_delta_load(location)
    filter_df = spark.sql("select distinct country,invoice_date from global_temp.incremental_load")
    incremental_df = full_source.join(filter_df,["country","invoice_date"],"leftsemi")
    return incremental_df

# COMMAND ----------

def incremental_analysis_view(df):
    df.groupBy("country", "invoice_year","invoice_month","invoice_date")\
        .agg(sf.round(sf.sum(sf.expr("quantity * unit_price")),2).alias("total_sales"))\
            .createOrReplaceTempView("incremental_analysis_tbl")


# COMMAND ----------

def merge_incremnetal_analysis():
    spark.sql(f"""merge into sales_delta_db.country_wise_daily_sales tg using 
              incremental_analysis_tbl sc on
              sc.country = tg.country and
               sc.invoice_year = tg.invoice_year
               and sc.invoice_month = tg.invoice_month
               and sc.invoice_date = tg.invoice_date 
               when matched then 
               update set tg.total_sales = sc.total_sales 
               when not matched then 
               insert * """)

# COMMAND ----------

if "initial" in v_load_type:
    df = full_delta_load('/mnt/lakehouse/silver/sales-delta/invoices/')
    df.createOrReplaceTempView("invoice_tbl_silver")
    full_analysis()
else:
    inc_df = load_incremental_source('/mnt/lakehouse/silver/sales-delta/invoices/')
    incremental_analysis_view(df)
    merge_incremnetal_analysis()



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from country_wise_daily_sales

# COMMAND ----------


