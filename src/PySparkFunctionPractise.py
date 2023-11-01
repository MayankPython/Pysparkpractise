from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from datetime import date

spark = SparkSession.builder \
    .master("local[*]")\
    .appName("TestSparkFunctions")\
    .getOrCreate()

df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True)\
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/order_data-201025-223502.csv") \
    .load()

print(date.today())