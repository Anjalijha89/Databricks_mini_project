# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

order_df = spark.read.csv(
    "/Volumes/workspace/default/customer_data/order.csv",
    header=True,
    inferSchema=True
)



# COMMAND ----------

df = spark.read.csv(
    "/Volumes/workspace/default/customer_data/customer.csv",
    header=True,
    inferSchema=True
)


# COMMAND ----------

display(order_df) 

# COMMAND ----------

order_df.printSchema()

# COMMAND ----------

customer_orders_df = df.join(order_df, 'Customer_ID', 'inner')


# COMMAND ----------

customer_orders_df.count()
customer_orders_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

# Total order per customer
# customer_orders_df.groupBy("Customer_ID").count().show()
customer_order_count=customer_orders_df.groupBy("Customer_ID").count().orderBy(col('count').desc())
customer_order_count.show(10)

# COMMAND ----------

from pyspark.sql.functions import sum

#Total spend per customer
customer_total_spend=customer_orders_df.groupBy('Customer_ID').agg(sum('Total_Amount')).orderBy(col("sum(Total_Amount)").desc())
customer_total_spend.show(10)


# COMMAND ----------

from pyspark.sql.functions import avg
customer_avg_spend =customer_orders_df.groupBy('Customer_ID').agg(avg('Total_Amount')).orderBy(col("avg(Total_Amount)").desc())
customer_avg_spend.show(10)

# COMMAND ----------

# order by status
order_status_count = customer_orders_df.groupBy('Status').count()
order_status_count.show()


# COMMAND ----------

customer_orders_df.display()

# COMMAND ----------

signup_by_month= customer_orders_df.withColumn('signup_month', month(col('Signup_Date')))\
    .groupby('signup_month')\
        .count()\
            .orderBy('signup_month')
signup_by_month.show()

# COMMAND ----------

from pyspark.sql import Window

window_spec = Window.orderBy(col('sum(Total_Amount)').desc())
ranked_customer = customer_total_spend.withColumn('dense_rank', dense_rank().over(window_spec))
ranked_customer.show(10)

# COMMAND ----------

customer_total_spend.printSchema(), customer_order_count.printSchema()

# COMMAND ----------

# Finding customer with High order frequency but low total spend
customers_spend_vs_orders = customer_order_count.join(customer_total_spend, 'customer_id','inner')\
    .orderBy(col('count').desc(),col('sum(Total_Amount)'))
customers_spend_vs_orders.show(5)


# COMMAND ----------

output_path = df.write.mode('overwrite').parquet('/Volumes/workspace/default/customer_data/final_customer_order_parquet')