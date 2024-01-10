from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .enableHiveSupport()\
    .getOrCreate()

customer_df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True)\
    .option("path", "s3://mayankbigdata/Input/customers-201025-223502.csv") \
    .load()

customer_df.write\
    .option("path", "s3n://mayankbigdata/customers/")\
    .format("parquet")\
    .partitionBy("customer_state")\
    .bucketBy(4, "customer_id")\
    .mode("overwrite")\
    .saveAsTable("customer_details")
