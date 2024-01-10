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

df.groupBy("Country", "InvoiceNo")\
    .agg(sum("Quantity").alias("total_quantity"),
         sum(expr("Quantity*UnitPrice")).alias("Invoice_value"))

#column String expression

df.groupBy("Country", "InvoiceNo")\
    .agg(expr("sum(Quantity) as total_quantity"),
         expr("sum(Quantity * UnitPrice) as Invoice_value"))

#spark SQL
df.createOrReplaceTempView("sales")

spark.sql("select Country, InvoiceNo, sum(Quantity) as total_quantity, sum(Quantity * UnitPrice) as Invoice_value "
          "from sales where total_quantity > 329 group by Country, InvoiceNo").show()

spark.stop()