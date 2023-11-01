from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()

