from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("IncrementalFileHandling")\
    .getOrCreate()

#Used the below code to genrate the output file.
"""
tempdf = spark.read\
    .format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
            "Week12-Apache Spark - Structured API - Part2/Datasets/order_data-201025-223502.csv")\
    .load()

tempdf.write\
    .mode("overwrite")\
    .partitionBy("InvoiceDate")\
    .parquet("C:/Users/mayan/OneDrive/Desktop/Big Data/Program output/orders")
"""
#this is we can take the todays date.
#todays_date = date.today()

todays_date = "05-12-2011"

df = spark.read\
    .parquet("C:/Users/mayan/OneDrive/Desktop/Big Data/Program output/orders/")\
    .filter(col("InvoiceDate") == todays_date)

df.write\
    .partitionBy("InvoiceDate")\
    .mode("overwrite")\
    .option("partitionOverwriteMode", "dynamic")\
    .parquet("C:/Users/mayan/OneDrive/Desktop/Big Data/Program output/orders_output/")
