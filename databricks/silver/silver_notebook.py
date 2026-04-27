# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
        .option("inferSchema", True)\
        .load('abfss://bronze@saanushridatalake.dfs.core.windows.net/rawdata')


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df= df.withColumn('Model_Category', split(col('Model_ID'),'-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

df.withColumn('Units_Sold', col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df=df.withColumn('RevPerUnit', col('Revenue')/col('Units_Sold'))
df.display()


# COMMAND ----------

display(df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year', 'Total_Units', ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','abfss://silver@saanushridatalake.dfs.core.windows.net/carsales').save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Quering Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@saanushridatalake.dfs.core.windows.net/carsales`