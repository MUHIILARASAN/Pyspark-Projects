# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

## create a spark session
spark = SparkSession.builder.appName('CustomerBehaviourAnalysis').getOrCreate()

# COMMAND ----------

## sample data

data = [
 (1, 101, "2023-01-05", 100.00),
 (2, 102, "2023-02-10", 150.00),
 (3, 103, "2023-03-10", 160.00),
 (4, 104, "2023-04-10", 180.00),
 (5, 105, "2023-05-10", 190.00),
 (6, 106, "2023-06-10", 120.00),
 (7, 107, "2023-07-10", 170.00),
 (8, 108, "2023-08-10", 130.00),
 (9, 109, "2023-04-07", 150.00),
 (10,110, "2023-06-10", 150.00)
]

# Define the schema
schema = ["customer_id", "order_id", "order_date", "total_order_value"]



# COMMAND ----------

## create a spark dataframe 
df = spark.createDataFrame(data = data, schema = schema)
df.show()

# COMMAND ----------

## convert order type to datatype

df = df.withColumn("order_date",F.to_date("order_date"))
df.show()

# COMMAND ----------

## calculate metrice 
metrics_df = (df.groupBy("customer_id").agg(
F.sum("total_order_value").alias("total_revenue"),
F.count("order_id").alias("purchase_count"),
F.max("order_date").alias("latest_order_date"),
F.min("order_date").alias("earliest_order_date")
)
)
metrics_df.show()

# COMMAND ----------

## calculate life time value (CLV)

metrics_df = metrics_df.withColumn("customer_life_time", F.col("total_revenue")/ F.datediff(F.col("latest_order_date"),F.col("earliest_order_date")))

# COMMAND ----------

### calculate purchase frequency
metrics_df = metrics_df.withColumn("purchase_frequency", F.col("purchase_count")/F.datediff(F.col("latest_order_date"),F.col("earliest_order_date")))

# COMMAND ----------

## calculate average order value

metrics_df = metrics_df.withColumn("average_order_value", F.col("total_revenue")/ F.col("purchase_count"))

# COMMAND ----------

## calculate churn rate
current_date = F.current_date()

window_spec = Window().partitionBy("customer_id").orderBy(F.col("order_date").desc())
last_order_date_df = df.withColumn("rank",F.row_number().over(window_spec)).filter(F.col("rank")==1)
metrics_df = metrics_df.withColumn("customer_churn_rate",F.when(F.datediff(current_date, F.col("latest_order_date")) > 90,1).otherwise(0))
## Display the result

metrics_df

# COMMAND ----------


