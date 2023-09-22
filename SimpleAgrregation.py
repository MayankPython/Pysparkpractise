from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()

df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True)\
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/order_data-201025-223502.csv") \
    .load()


#Column object expression.
"""
df.select(count("*").alias("row_count"),
          sum("Quantity").alias("total_quantity"),
          avg("UnitPrice").alias("avg_price"),
          countDistinct("InvoiceNo").alias("count_distinct")
          ).show()
"""

#column String expression
"""
df.selectExpr("count(*) as row_count",
              "sum(Quantity) as total_quantity",
              "avg(UnitPrice) as avg_price",
              "count(Distinct(InvoiceNo)) as count_distinct"
              ).show()
"""

#spark SQL

df.createOrReplaceTempView("sales")

spark.sql("select count(*) as row_count,sum(Quantity) as total_quantity,avg(UnitPrice) as avg_price,"
          "count(Distinct(InvoiceNo)) as count_distinct from sales").show()

spark.stop()