from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode,count,col
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("WordCount")\
    .getOrCreate()
path = "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week09 - Apache Spark/Datasets/" \
       "search_data.txt"
df = spark.read.text(path)
df1 = df.select(split(df.value," ").alias("value_list")).show()
#df2 = df1.select(explode("value_list").alias("words"))
#df3 = df2.groupBy("words").agg(count("*").alias("count")).orderBy(col("count").desc()).show()