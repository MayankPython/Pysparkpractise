from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()

dept_df = spark.read.format("json") \
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/dept.json") \
    .load()

employee_df = spark.read.format("json") \
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week12-Apache Spark - Structured API - Part2/Datasets/employee.json") \
    .load()

join_condition = dept_df.deptid == employee_df.deptid

joined_df = employee_df.join(dept_df,join_condition,"inner").drop(employee_df.deptid)
#Below is using SQL expression.
joined_df.createOrReplaceTempView("emp_table")
spark.sql("select deptName, deptid, count(id) as empcount from emp_table group by deptName, deptid").show()

#Below is jusing string expression.
joined_df.groupBy("deptName", "deptid")\
    .agg(expr("count(id) as empcount")).show()

#Below is using column object

joined_df.groupBy("deptName", "deptid")\
    .count().alias("empcount").show()

joined_df.write.format("parquet").partitionBy("deptName").saveAsTable("my_test")