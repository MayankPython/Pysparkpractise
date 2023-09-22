from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id, concat_ws,collect_list
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()

mylist = [("username1", "friend1"),
  ("username1", "friend2"),
  ("username2", "friend1"),
  ("username2", "friend3")]

df = spark.createDataFrame(mylist).toDF("username", "friends")

df2 = df.groupBy("username").agg(collect_list("friends").alias("friends"))
df2.count().show()
"""
+---------+------------------+
| username|           friends|
+---------+------------------+
|username1|[friend1, friend2]|
|username2|[friend1, friend3]|
+---------+------------------+
"""
df3 = df2.withColumn("friends", concat_ws(",", "friends"))
df3.show()
"""
+---------+---------------+
| username|        friends|
+---------+---------------+
|username1|friend1,friend2|
|username2|friend1,friend3|
+---------+---------------+
"""
