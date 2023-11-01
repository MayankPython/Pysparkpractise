from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practise") \
    .getOrCreate()

customer_df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True)\
    .option("path", "s3n://big-data-mayank-bucket/customers-201025-223502.csv") \
    .load()

orders_df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("header", True)\
    .option("path", "s3n://big-data-mayank-bucket/orders-201025-223502.csv") \
    .load()

orders_df.printSchema()
join_condition = customer_df.customer_id == orders_df.order_customer_id

join_df = customer_df.join(orders_df, join_condition, "inner")

join_df.show()
