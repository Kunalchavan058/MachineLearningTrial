# Databricks notebook source
# MAGIC %pip install ephem
# MAGIC %pip install pvlib

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %fs ls mnt/ML

# COMMAND ----------

# File path
TimeInterval_path = 'dbfs:/mnt/ML/TimeStamps.csv'
# Read data from CSV and infer schema
TimeInterval = spark.read.format('csv').option('header', 'true').load(TimeInterval_path)


# COMMAND ----------

display(TimeInterval)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
import ephem

# Initialize SparkSession
spark = SparkSession.builder.appName("SunriseSunset").getOrCreate()

# File path
file_path = 'dbfs:/mnt/ML/SunriseSunset.csv'

# Read data from CSV and infer schema
Plant_location = spark.read.format('csv').option('header', 'true').load(file_path)


# COMMAND ----------

display(Plant_location)

# COMMAND ----------

joined_df = TimeInterval.join(Plant_location, on = 'PlantID', how = 'inner')
joined_df = joined_df.select('PlantID', 'Timestamp', 'Longitude', 'Latitude', 'TimeZone', 'Date')
display(joined_df)

# COMMAND ----------



# COMMAND ----------


from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType


# Function to calculate sunrise and sunset using ephem
def calculate_sunrise_sunset(longitude, latitude, date):
    obs = ephem.Observer()
    obs.lon = str(longitude)
    obs.lat = str(latitude)
    obs.date = str(date)
    sunrise = ephem.localtime(obs.previous_rising(ephem.Sun())).strftime('%Y-%m-%dT%H:%M:%SZ')
    sunset = ephem.localtime(obs.next_setting(ephem.Sun())).strftime('%Y-%m-%dT%H:%M:%SZ')
    return sunrise, sunset

# Register the function as a UDF with a StructType output
schema = StructType([
    StructField("Sunrise", StringType(), True),
    StructField("Sunset", StringType(), True)
])

calculate_sunrise_sunset_udf = udf(calculate_sunrise_sunset, schema)

# Apply the UDF to create a new column with struct type containing sunrise and sunset
df = joined_df.withColumn("SunriseSunset", calculate_sunrise_sunset_udf("Longitude", "Latitude", "Date"))

# Extract values from the struct column to separate Sunrise and Sunset columns
df = df.withColumn("Sunrise", df["SunriseSunset"].getField("Sunrise"))
df = df.withColumn("Sunset", df["SunriseSunset"].getField("Sunset"))

# Drop the intermediate struct column if needed
df = df.drop("SunriseSunset")
df = df.limit(1000)
# Cast 'Longitude' and 'Latitude' columns to DoubleType
df = df.withColumn("Longitude", col("Longitude").cast(DoubleType()))
df = df.withColumn("Latitude", col("Latitude").cast(DoubleType()))


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql.functions import col
import pvlib

# Function to calculate sun position and return as an array
def calculate_sun_position(timestamp, longitude, latitude):
    solar_pos = pvlib.solarposition.get_solarposition(
        timestamp,
        latitude,
        longitude
    )
    return [
        float(solar_pos['apparent_zenith']),
        float(solar_pos['apparent_elevation']),
        float(solar_pos['azimuth'])
    ]

# Register UDF
calculate_sun_position_udf = udf(calculate_sun_position, ArrayType(DoubleType()))


# Apply UDF to calculate sun position for each row
result_df = df.withColumn(
    "SunPosition",
    calculate_sun_position_udf('Timestamp', 'Longitude', 'Latitude')
)

# Explode the 'SunPosition' array column to create three new columns
result_df = result_df.withColumn("zenith", col("SunPosition").getItem(0)) \
                     .withColumn("elevation", col("SunPosition").getItem(1)) \
                     .withColumn("azimuth", col("SunPosition").getItem(2))

result_df = result_df.drop('SunPosition')

display(result_df)

# COMMAND ----------

# Filter values where 'Timestamp' is between 'Sunrise' and 'Sunset'
filtered_night_time = result_df.filter((col('Timestamp') >= col('Sunrise')) & (col('Timestamp') <= col('Sunset')))

# Show the filtered DataFrame
filtered_night_time.show()
