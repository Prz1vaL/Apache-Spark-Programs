from pyspark.sql import SparkSession , functions as func

# For Unstructured Data Types
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

# Create Spark Session
spark = SparkSession.builder.appName("Temperature").getOrCreate()

# Create Schema
schema = StructType([ \
    StructField("stationID",StringType(),True), \
    StructField("date",IntegerType(),True), \
    StructField("measure_type",StringType(),True), \
    StructField("temperature",FloatType(),True)])

# Load Data as DataFrame
df = spark.read.schema(schema).csv("1800.csv")
df.printSchema()

# Filter Data out all but T-MIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID","temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# Convert temperature to Fahrenheit and sort the dataset
minTempsByStationF = minTempsByStation.withColumn("temperature",func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0,2)).select("stationID","temperature").sort("temperature")

# Show the results
results = minTempsByStationF.collect()

for result in results :
    print(result[0] + "\t{:.2f}F".format(result[1]))


