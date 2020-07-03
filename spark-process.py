# Import libraries
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# Create spark context
spark = SparkSession.builder.appName('Data_Wrangling').getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)

# Create dataframes
df_highway = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("gs://spark-analysis/Dataset/highway_data.csv"))

df_detector = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("gs://spark-analysis/Dataset/detector_metadata.csv"))

df_station = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("gs://spark-analysis/Dataset/station_metadata.csv"))

# Drop unwanted columns
df_detector = df_detector.drop("milepost", "detectortitle", "lanenumber", "agency_lane", "active_dates")
df_station = df_station.drop("milepost","length", "numberlanes", "agencyid", "active_dates")

# Rename detector_id on one table so that join works
df_highway = df_highway.withColumnRenamed('detector_id','detectorid')

# Join the dataframes
df_first = df_highway.join(df_detector, on=['detectorid'], how='full')

# Drop highwayid to avoid duplicate
df_first = df_first.drop("highwayid")

# Join to get the final result
df = df_first.join(df_station, on=['stationid'], how='full')

# Show the result
df.show()

# Convert to CSV
df.coalesce(1).write.option("header", "true").csv("gs://spark-analysis/Dataset/processed_data.csv")
