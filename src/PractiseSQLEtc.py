from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("Practise")\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.read\
    .option("inferSchema", True)\
    .option("header", True)\
    .csv("C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/orders-201025-223502.csv")

df.printSchema()

df.write.format("csv")\
    .mode("overwrite")\
    .saveAsTable("orders1")

spark.sql("create database if not exists retail")

#bucketBy can be used only for table.
df.write.format("csv") \
    .mode(saveMode="overwrite") \
    .bucketBy(4, "order_customer_id")\
    .sortBy("order_customer_id")\
    .saveAsTable("retail.orders4")

spark.stop()