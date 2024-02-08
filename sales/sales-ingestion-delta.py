# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
import pyspark.sql.functions as sf
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("p_load_type","")
dbutils.widgets.text("p_load_date","")

v_load_type = dbutils.widgets.get("p_load_type")
v_load_date = dbutils.widgets.get("p_load_date")

# COMMAND ----------



# COMMAND ----------

def get_invoice_schema():
    return StructType([StructField("InvoiceNo",StringType()),
                       StructField("StockCode",StringType()),
                       StructField("Description",StringType()),
                       StructField("Quantity",StringType()),
                       StructField("InvoiceDate",StringType()),
                       StructField("UnitPrice",StringType()),
                       StructField("CustomerID",StringType()),
                       StructField("Country",StringType()),
                                   ])



# COMMAND ----------

def load_csv_data(file_path,file_schema):
    return spark.read\
        .format("csv")\
            .option("mode","FAILFAST")\
                .option("header",True)\
                    .schema(file_schema)\
                        .load(file_path)

# COMMAND ----------

def rename_columns(df):
    return df.withColumnRenamed("StockCode","stock_code")\
            .withColumnRenamed("InvoiceDate","invoice_date")\
            .withColumnRenamed("Description","description")\
            .withColumnRenamed("Quantity","quantity")\
            .withColumnRenamed("UnitPrice","unit_price")\
            .withColumnRenamed("CustomerID","customer_id")\
            .withColumnRenamed("Country","country")\
            .withColumnRenamed("InvoiceNo","invoice_no")

# COMMAND ----------

def data_type_correction(df):
    return df.withColumn("invoice_date",sf.to_date("invoice_date",'d-M-y h.m'))

# COMMAND ----------

def add_fields(df):
    return df.withColumn("invoice_year",sf.year("invoice_date"))\
            .withColumn("invoice_month",sf.month("invoice_date"))

# COMMAND ----------

def transform_invoices(df):
    ren_df = rename_columns(df)
    dt_df = data_type_correction(ren_df)
    return add_fields(dt_df)

# COMMAND ----------

def save_invoices(df,file_path):
    df.write\
        .format("delta")\
            .mode("overwrite")\
                .partitionBy("country","invoice_date")\
                    .save(file_path)

# COMMAND ----------

def merge_invoices(df,target_location):
    delta_table = DeltaTable.forPath(spark,target_location)
    delta_table.alias("tr").merge(df.alias("src"),"src.invoice_no = tr.invoice_no and src.stock_code = tr.stock_code\
        and src.invoice_date = tr.invoice_date and src.country = tr.country").whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()

# COMMAND ----------

file_schema = get_invoice_schema()
if "initial" in v_load_type.lower():
    file_path = '/mnt/lakehouse/bronze/sales-delta/historic-data'
    df = load_csv_data(file_path,file_schema)
    transformed_df = transform_invoices(df)
    save_invoices(transformed_df,'/mnt/lakehouse/silver/sales-delta/invoices/')
else:
    file_path = f"/mnt/lakehouse/bronze/sales-delta/incremental-data/invoices_{v_load_date}.csv"
    df = load_csv_data(file_path,file_schema)
    transformed_df = transform_invoices(df)
    merge_invoices(transformed_df,'/mnt/lakehouse/silver/sales-delta/invoices/')
    transformed_df.createOrReplaceGlobalTempView("incremental_load")


# COMMAND ----------

df = DeltaTable.forPath(spark,'/mnt/lakehouse/silver/sales-delta/invoices/').toDF()
df.filter("country == 'United Kingdom' and invoice_year = 2022").createOrReplaceTempView("UK_view")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select country, invoice_month, invoice_year, round(sum(quantity*unit_price),2) as total_price from uk_view
# MAGIC group by country, invoice_month , invoice_year

# COMMAND ----------


