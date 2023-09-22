from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("testMapFlatMap").getOrCreate()

mylist =[("Project Gutenberg's",),
       ("Alice’s Adventures in Wonderland",),
       ("Project Gutenberg’s",),
       ("Adventures in Wonderland",),
       ("Project Gutenberg’s",)]
schema = ["data"]
df = spark.createDataFrame(mylist,schema=schema).show()
#df don't have map() and flatMap() attribute.
#rdd1 = df.rdd.map(lambda x: x.split(" "))
#print(rdd1.collect())