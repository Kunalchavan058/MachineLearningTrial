# Databricks notebook source
pip install databricks-feature-store

# COMMAND ----------

# MAGIC %pip install --upgrade koalas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

dbutils.fs.mount(
    source="wasbs://airbnbdata@obungitrialdatastorage.blob.core.windows.net/",
    mount_point="/mnt/ML",
    extra_configs={
        "fs.azure.account.key.obungitrialdatastorage.blob.core.windows.net": "7GOxsWV+sWjrZsGdxO9g+HYfEOzO+s2lXPZZP9NG4VnzkZNwH5kD9GBlhr+BfuJqI8EEZ39wB6Tw+AStdwKYDw=="
    }
)

# COMMAND ----------

# MAGIC %fs ls /mnt/ML

# COMMAND ----------

file_path = 'dbfs:/mnt/ML/Airbnb_Booking_data.csv'
# Define the columns to select
selected_columns = ["price", "number_of_reviews", "reviews_per_month", "availability_365", "neighbourhood"]

Airbnbdata = spark.read \
    .format('csv') \
    .option('header', 'true') \
    .load(file_path)\
    .select(selected_columns)
Airbnbdata = Airbnbdata.dropna(how='any')
Airbnbdata = Airbnbdata.select([col(c).cast("int") for c in Airbnbdata.columns])

# COMMAND ----------

display(Airbnbdata)

# COMMAND ----------


spark.sql("CREATE DATABASE IF NOT EXISTS AirbnbStandardize")
spark.sql("USE AirbnbStandardize")

# COMMAND ----------

Airbnbdata.write.format("delta").saveAsTable("Airbnb_standardize")

# COMMAND ----------

from databricks.feature_store import feature_table




# COMMAND ----------


