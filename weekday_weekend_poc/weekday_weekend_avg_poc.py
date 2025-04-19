# Databricks notebook source
customer_data_df = \
    spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True)\
    .load("/FileStore/tables/customer_data-1.csv")

# COMMAND ----------

customer_data_df.show()

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

transformed_df = customer_data_df.withColumn(
    "order_date_transformed", F.date_format(F.col("order_date").cast("date"), "E")
)

# COMMAND ----------

transformed_df.show()

# COMMAND ----------

transformed_df2 = transformed_df.withColumn(
    "days",
    F.when(F.col("order_date_transformed").isin("Sat", "Sun"), "Weekend").otherwise(
        "Weekdays"
    ),
)

# COMMAND ----------

transformed_df3 = transformed_df2.groupBy("days").agg(F.avg("amount").alias("avg_amount"))

# COMMAND ----------

transformed_df3.show()