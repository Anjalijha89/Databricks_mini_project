# Databricks notebook source
# MAGIC %md
# MAGIC ## Read and Process Data

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CustomerDataProcessing").getOrCreate()
spark

# COMMAND ----------

df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Volumes/workspace/default/customer_data/customer.csv")
df.show(5)

# COMMAND ----------

df = spark.read.csv(
    "/Volumes/workspace/default/customer_data/customer.csv",
    header=True,
    inferSchema=True
)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn(
    'Signup_Date',
    to_date(col('Signup_Date'), 'yyyy-MM-dd')
)

# COMMAND ----------

df.show(5)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### df.fillna() is used in PySpark to replace NULL/missing values in a DataFrame.

# COMMAND ----------

df = df.fillna({'City':'Unknown','State':'Unknown'})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add single column from already existing column

# COMMAND ----------

df = df.withColumn("Signup_Year",year(col('Signup_Date')))
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add multiple column from already existing column

# COMMAND ----------

df = df.withColumn("Signup_Year",year(col('Signup_Date')))\
    .withColumn("Signup_Month",month(col('Signup_Date')))
df.show(5)

# COMMAND ----------

unique_country = df.select(countDistinct('State')).collect()
print(unique_country[0][0])


# COMMAND ----------

# MAGIC %md
# MAGIC #### Count how many customer in each city

# COMMAND ----------

df.groupBy('City').count().orderBy(col('count')).show()

# COMMAND ----------

df.groupBy('State') \
  .count() \
  .orderBy(col('count').desc()) \
  .show(100, truncate=False)

# COMMAND ----------

df.groupBy('City','State') \
  .count() \
  .orderBy(col('count').desc()) \
  .show(100, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pivot Table count of Active INACTIVE AND Pending user per city

# COMMAND ----------

# DBTITLE 1,Cell 20
df.groupBy('City').pivot('Status').count().show()

# COMMAND ----------

df.show(5)

# COMMAND ----------

# DBTITLE 1,Cell 22
from pyspark.sql.window import Window

window_spec = Window.partitionBy('State').orderBy(col('Signup_Date').desc())
df = df.withColumn('rank',rank().over(window_spec))\
    .withColumn('dense_rank',dense_rank().over(window_spec))\
        .withColumn('row_number',row_number().over(window_spec))


# COMMAND ----------

df.show(5)

# COMMAND ----------

df_recent_customer=df.filter(col('Signup_Date') > lit('2023-11-01'))
# df_recent_customer.show()
df_recent_customer.count()

# COMMAND ----------

# oldest and the newest customer per city
df.groupBy('City').agg(min('Signup_Date').alias('Oldest'),max("Signup_Date").alias('Newest')).show()

# COMMAND ----------

df.write.mode('overwrite').parquet('/Volumes/workspace/default/customer_data/customer_parquet')