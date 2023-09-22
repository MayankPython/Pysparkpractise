from datetime import date
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()


mylist = [ (1, date(2016, 1, 7), 13.90),
           (1, date(2016, 1, 16), 14.50),
           (2, date(2016, 1, 9), 10.50),
           (2, date(2016, 1, 28), 5.50),
           (3, date(2016, 1, 5), 1.50)]

df = spark.createDataFrame(mylist).toDF("id", "date", "price")
df.repartition()
print(df.rdd.getNumPartitions())

df.createOrReplaceTempView("temp_view")
df1 = spark.sql("select * from (select *, dense_rank() over (partition by id order by date desc) as rank "
                "from temp_view) where rank = 1").drop("rank")
df1.show()
