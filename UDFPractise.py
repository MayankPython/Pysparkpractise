from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()


def age_check(age):
    if age > 18:
        return "Y"
    else:
        return "N"


df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/-201025-223502.dataset1") \
    .load()

df1 = df.toDF("name", "age", "city")

"""
This is column object expression.
parseAgeFunction = udf(age_check, StringType())

df2 = df1.withColumn("adult", parseAgeFunction("age"))

df2.printSchema()
df2.show()

"""

#This is for SQL expression. UDF will be registered to catalog and function can be used for sql expressions.

spark.udf.register("parseAgeFunction", age_check,StringType())

df2 = df1.withColumn("adult", expr("parseAgeFunction(age)"))

df2.printSchema()
df2.show()

spark.stop()
