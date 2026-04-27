# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------

df_silver = spark.sql("select * from parquet.`abfss://silver@saanushridatalake.dfs.core.windows.net/carsales`")
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading all the dim tables

# COMMAND ----------

df_model = spark.sql('select * from cars_catalog.gold.dim_model')

df_branch = spark.sql('select * from cars_catalog.gold.dim_branch')

df_dealer = spark.sql('select * from cars_catalog.gold.dim_dealer')

df_date = spark.sql('select * from cars_catalog.gold.dim_date')

# COMMAND ----------

# MAGIC %md
# MAGIC # Bringing Keys to the fact table

# COMMAND ----------

df_fact = df_silver.join(df_model, df_silver['Model_ID'] == df_model['Model_ID'], how='left')\
    .join(df_branch, df_silver['Branch_ID'] == df_branch['Branch_ID'], how='left')\
    .join(df_dealer, df_silver['Dealer_ID'] == df_dealer['Dealer_ID'], how='left')\
    .join(df_date, df_silver['Date_ID'] == df_date['Date_ID'], how='left')\
    .select(df_silver['Revenue'], df_silver['Units_Sold'], df_silver['RevPerUnit'], df_branch['dim_branch_key'], df_model['dim_model_key'], df_dealer['dim_dealer_key'], df_date['dim_date_key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('fact_sales'):
    delta_tbl = DeltaTable.forName(spark, 'cars_catalog.gold.fact_sales')

    delta_tbl.alias('t').merge(df_fact.alias('s'),'t.dim_branch_key = s.dim_branch_key and t.dim_model_key = s.dim_model_key and t.dim_dealer_key = s.dim_dealer_key and t.dim_date_key = s.dim_date_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_fact.write.format('delta')\
        .mode('Overwrite')\
        .option('path', 'abfss://gold@saanushridatalake.dfs.core.windows.net/fact_sales')\
        .saveAsTable('cars_catalog.gold.fact_sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.fact_sales