# Databricks notebook source
# MAGIC %pip install ephem

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
import ephem

# Initialize SparkSession
spark = SparkSession.builder.appName("SunriseSunset").getOrCreate()

# File path
file_path = 'dbfs:/mnt/ML/Sunrise_sunset.csv'

# Read data from CSV and infer schema
SunriseSunset = spark.read.format('csv').option('header', 'true').load(file_path)

# Function to calculate sunrise and sunset using ephem
def calculate_sunrise_sunset(longitude, latitude, date):
    obs = ephem.Observer()
    obs.lon = str(longitude)
    obs.lat = str(latitude)
    obs.date = date
    sunrise = ephem.localtime(obs.previous_rising(ephem.Sun())).strftime('%Y-%m-%d %H:%M:%S')
    sunset = ephem.localtime(obs.next_setting(ephem.Sun())).strftime('%Y-%m-%d %H:%M:%S')
    return sunrise, sunset

# Register the function as a UDF with a StructType output
schema = StructType([
    StructField("Sunrise", StringType(), True),
    StructField("Sunset", StringType(), True)
])

calculate_sunrise_sunset_udf = udf(calculate_sunrise_sunset, schema)

# Apply the UDF to create a new column with struct type containing sunrise and sunset
df = SunriseSunset.withColumn("SunriseSunset", calculate_sunrise_sunset_udf("Longitude", "Latitude", "Date"))

# Extract values from the struct column to separate Sunrise and Sunset columns
df = df.withColumn("Sunrise", df["SunriseSunset"].getField("Sunrise"))
df = df.withColumn("Sunset", df["SunriseSunset"].getField("Sunset"))

# Drop the intermediate struct column if needed
df = df.drop("SunriseSunset")



