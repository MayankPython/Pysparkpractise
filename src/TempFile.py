from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("TempApp").getOrCreate()

df = spark.read.format("csv").option("inferSchema", True).option("path", "path").load()

spark.sql("select ")