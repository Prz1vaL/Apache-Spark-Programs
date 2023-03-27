# Import the necessary modules and libraries
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create a SparkSession
spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

# Create schema when reading customer-orders.csv
schema = StructType([ \
    StructField("cust_id", IntegerType(), True), \
    StructField("item_id", IntegerType(), True), \
    StructField("amount_spent", FloatType(), True)])

# Load up the data into a DataFrame
df = spark.read.schema(schema).csv("customer-orders.csv")

# Group by customer ID and sum up the amount spent
df.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent")).sort("total_spent").show()

# Group by total amount spent by customer
df.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent")).sort(func.desc("total_spent")).show()

# Stop the session
spark.stop()



