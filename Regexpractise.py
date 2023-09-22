from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("Practise")\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.read.text("C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                     "Week12-Apache Spark - Structured API - Part2/Datasets/orders-new.txt")

my_regex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

final_df = df.select(regexp_extract("value",my_regex,1).alias("order_id"),
                     regexp_extract("value",my_regex,2).alias("date"),
                     regexp_extract("value",my_regex,3).alias("customer_id"),
                     regexp_extract("value",my_regex,4).alias("status"))

final_df.printSchema()

final_df.show()

spark.stop()