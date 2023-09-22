from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()

mylist = [(1,"2013-07-25",11599,"CLOSED"),
          (2,"2013-07-25",256,"PENDING_PAYMENT"),
          (3,"2013-07-25",12111,"COMPLETE"),
          (4,"2013-07-25",8827,"CLOSED"),
          (7,"2013-07-25",12111,"COMPLETE"),
          (5,"2013-07-25",11318,"COMPLETE"),
          (6,"2013-07-25",7130,"COMPLETE")
          ]

df1 = spark.createDataFrame(mylist)\
    .toDF("orderid", "date","customerid","status")

df2 = df1.withColumn("data_unix_time",unix_timestamp("date", "yyyy-mm-dd"))\
    .withColumn("newid", monotonically_increasing_id())\
    .dropDuplicates(["date","customerid"])\
    .drop("orderid")\
    .sort("date")

df2.printSchema()
df2.show()
df2.filter()
df2.sort()
df2.drop()
df2.dropDuplicates
df2.unionAll
df2.sort()
df2.union()
df2.orderBy()
df2.withColumn()
df2.withColumnRenamed()
df2.replace()
df2.foreach()
df2.fillna()