from pyspark.sql import SparkSession
#from pyspark import StorageLevel
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()

dept_df = spark.read.format("json") \
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/dept.json") \
    .load()

employee_df = spark.read.format("json") \
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/employee.json") \
    .load()
employee_df.unpersist()
#    .load().persist(StorageLevel.MEMORY_AND_DISK)
