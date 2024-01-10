from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.adaptive.enabled", "false")


df_uniform = spark.createDataFrame([i for i in range(1000000)], IntegerType())
df_uniform.show(5, False)

df_uniform\
    .withColumn("partition", F.spark_partition_id())\
    .groupBy("partition")\
    .count()\
    .orderBy("partition")\
    .show(15, False)

df0 = spark.createDataFrame([0] * 999990, IntegerType()).repartition(1)
df1 = spark.createDataFrame([1] * 15, IntegerType()).repartition(1)
df2 = spark.createDataFrame([2] * 10, IntegerType()).repartition(1)
df3 = spark.createDataFrame([3] * 5, IntegerType()).repartition(1)
df_skew = df0.union(df1).union(df2).union(df3)
df_skew.show(5, False)

df_skew\
    .withColumn("partition", F.spark_partition_id())\
    .groupBy("partition")\
    .count()\
    .orderBy("partition")\
    .show()

df_joined_c1 = df_skew.join(df_uniform, "value", 'inner')


df_joined_c1\
    .withColumn("partition", F.spark_partition_id())\
    .groupBy("partition")\
    .count()\
    .show(5, False)

# +---------+-------+
# |partition|count  |
# +---------+-------+
# |0        |1000005|
# |1        |15     |
# +---------+-------+

SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))

df_skew = df_skew.withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))

df_skew.show(10, truncate=False)

# +-----+----+
# |value|salt|
# +-----+----+
# |0    |2   |
# |0    |0   |
# |0    |0   |
# |0    |1   |
# |0    |2   |
# |0    |2   |
# |0    |0   |
# |0    |2   |
# |0    |2   |
# |0    |1   |
# +-----+----+

df_uniform = (
    df_uniform
    .withColumn("salt_values", F.array([F.lit(i) for i in range(SALT_NUMBER)]))
    .withColumn("salt", F.explode(F.col("salt_values")))
)

df_uniform.show(10, truncate=False)

# +-----+-----------+----+
# |value|salt_values|salt|
# +-----+-----------+----+
# |0    |[0, 1, 2]  |0   |
# |0    |[0, 1, 2]  |1   |
# |0    |[0, 1, 2]  |2   |
# |1    |[0, 1, 2]  |0   |
# |1    |[0, 1, 2]  |1   |
# |1    |[0, 1, 2]  |2   |
# |2    |[0, 1, 2]  |0   |
# |2    |[0, 1, 2]  |1   |
# |2    |[0, 1, 2]  |2   |
# |3    |[0, 1, 2]  |0   |
# +-----+-----------+----+

df_joined = df_skew.join(df_uniform, ["value", "salt"], 'inner')

(
    df_joined
    .withColumn("partition", F.spark_partition_id())
    .groupBy("value", "partition")
    .count()
    .orderBy("value", "partition")
    .show()
)

# +-----+---------+------+
# |value|partition| count|
# +-----+---------+------+
# |    0|        0|332774|
# |    0|        1|333601|
# |    0|        2|333615|
# |    1|        0|     6|
# |    1|        1|     9|
# |    2|        0|     2|
# |    2|        1|     2|
# |    2|        2|     6|
# |    3|        0|     3|
# |    3|        1|     2|
# +-----+---------+------+


###########################Salting In Aggregations########################################

df_skew.groupBy("value").count().show()

# +-----+------+
# |value| count|
# +-----+------+
# |    0|999990|
# |    2|    10|
# |    3|     5|
# |    1|    15|
# +-----+------+

(
    df_skew
    .withColumn("salt", (F.rand() * SALT_NUMBER).cast("int"))
    .groupBy("value", "salt")
    .agg(F.count("value").alias("count"))
    .groupBy("value")
    .agg(F.sum("count").alias("count"))
    .show()
)

spark.stop()