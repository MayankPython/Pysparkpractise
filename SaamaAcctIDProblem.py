from pyspark.sql import SparkSession
from pyspark.sql.functions import count
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("problem")\
    .getOrCreate()

mylist = [[101, 1, 0, 0],
          [101, 1, 0, 0],
          [101, 0, 0, 1]]

df = spark.createDataFrame(mylist).toDF("acct_id", "a1", "b1", "c1")
df1 = df.createOrReplaceTempView("tempview")
df1 = spark.sql("select a1, b1, c1 from tempview group by a1, b1, c1 having count(a1) = 1 or count(b1) = 1 "
                "or count(c1) = 1").show()
#df1 = df.groupBy("a1").agg(count("*").alias("count")).filter("a1==1").filter("count == 1")