"""
PySpark Joins -
    Inner Join
    Left / Left Outer Join
    Right / Right Outer Join
    Outer / Full Join
    Cross Join
    Left Anti Join
    Left Semi Join
    Self Join
"""
"""
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("InnerJoinExample").getOrCreate()

# Create the first dataframe
df1 = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], ["letter", "number"])
# Create the second dataframe
df2 = spark.createDataFrame([("A", 4), ("B", 5), ("D", 6)], ["letter", "value"])
# Perform the inner join
inner_join = df1.join(df2, df1['letter'] == df2['letter'], "inner")
# Show the result of the join
inner_join.show()

# Output:
# +-----+------+-----+
# |letter|number|value|
# +-----+------+-----+
# |    A|     1|    4|
# |    B|     2|    5|
# +-----+------+-----+
"""
"""   Left / Left Outer Join

df1.join(df2, df1['key'] == df2['key'], 'left').show()
#(OR)
df1.join(df2, df1['key'] == df2['key'], 'left_outer').show()

# Output:
# +-----+------+-----+
# |letter|number|value|
# +-----+------+-----+
# |    A|     1|    4|
# |    B|     2|    5|
# |    C|     3| null|
# +-----+------+-----+


Right / Right Outer Join

df1.join(df2, df1['key'] == df2['key'], 'right').show()
(OR)
df1.join(df2, df1['key'] == df2['key'], 'right_outer').show()

 Output:
# +-----+------+-----+
# |letter|number|value|
# +-----+------+-----+
# |    A|     1|    4|
# |    B|     2|    5|
# |    D|  null|    6|
# +-----+------+-----+

Outer / Full Join

df1.join(df2, df1['key'] == df2['key'], 'outer').show()
(OR)
df1.join(df2, df1['key'] == df2['key'], 'full').show()
(OR)
df1.join(df2, df1['key'] == df2['key'], 'full_outer').show()

# Output:
# +-----+------+-----+
# |letter|number|value|
# +-----+------+-----+
# |    A|     1|    4|
# |    B|     2|    5|
# |    C|     3| null|
# |    D|  null|    6|
# +-----+------+-----+

Cross Join

# Create the first dataframe
df1 = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], ["letter", "number"])

# Create the second dataframe
df2 = spark.createDataFrame([("X", 4), ("Y", 5), ("Z", 6)], ["symbol", "value"])

# Perform the cross join
cross_join = df1.crossJoin(df2)


# Output:
# +-----+------+------+-----+
# |letter|number|symbol|value|
# +-----+------+------+-----+
# |    A|     1|    X |   4 |
# |    A|     1|    Y |   5 |
# |    A|     1|    Z |   6 |
# |    B|     2|    X |   4 |
# |    B|     2|    Y |   5 |
# |    B|     2|    Z |   6 |
# |    C|     3|    X |   4 |
# |    C|     3|    Y |   5 |
# |    C|     3|    Z |   6 |
# +-----+------+------+-----+

Self Join

df = spark.createDataFrame([("A", 1), ("B", 2), ("C", 3)], ["letter", "number"])

# Perform the self join
self_join = df.alias("df1").join(df.alias("df2"), df["letter"] == df["letter"])

# Show the result of the join
self_join.show()

# Output:
# +-----+------+-----+------+
# |letter|number|letter|number|
# +-----+------+-----+------+
# |    A|     1|    A|     1|
# |    B|     2|    B|     2|
# |    C|     3|    C|     3|
# +-----+------+-----+------+

"""

