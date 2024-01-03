from pyspark.sql import SparkSession
from pyspark.sql.functions import xpath

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()
