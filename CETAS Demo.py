# Databricks notebook source
fire_df=spark.read\
          .format("csv")\
          .option("header","true")\
          .option("inferSchema","true")\
          .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(fire_df)

# COMMAND ----------

# to get all the columns in list
columns=fire_df.columns
print(columns)

# COMMAND ----------


# loop all the columns, and remove the spaces in between using replace method and store it in list
renamed_list=[]
for i in columns:
    renamed_list.append(i.replace(" ",""))
    

# COMMAND ----------

print(renamed_list)

# COMMAND ----------

print(type(renamed_list))

# COMMAND ----------

removed_whitespace_df=fire_df.toDF(*renamed_list)

# COMMAND ----------

removed_whitespace_df.printSchema()

# COMMAND ----------

display(removed_whitespace_df)

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------

spark.sql("create database if not exists pravin")

# COMMAND ----------

removed_whitespace_df.write.mode("append").saveAsTable("pravin.fire_tbl")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/pravingm/ext/

# COMMAND ----------

df = spark.sql("SELECT * FROM fire_tmp_vw")
#df.write.mode("append").parquet("dbfs:/FileStore/pravingm/ext/")
df.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/pravingm/ext/")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sales_data_parquet;
# MAGIC CREATE TABLE sales_data_parquet
# MAGIC USING parquet
# MAGIC OPTIONS (path "dbfs:/FileStore/pravingm/ext/")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM sales_data_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT *) FROM sales_data_parquet