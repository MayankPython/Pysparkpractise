#Read the file where we have movies rating. Columns are, User-id, Movie-id, Rating and Unix timestamp
#Read the file contain movies information. Columns are, Movie-id, Movie-name, Catagory.
#Get the top rated movies where there are atleast 100 rating.

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, col
spark = SparkSession.builder\
    .appName("TopMovieRatings")\
    .master("local[*]")\
    .getOrCreate()

movie_rating = spark.read\
    .format("text")\
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week11-Apache Spark - Structured API - Part1/Datasets/ratings-201019-002101.dat")\
    .load()

df = movie_rating.selectExpr("split(value, '::') as temp")
df2 = df.withColumn("movie_id", df.temp[1])\
    .withColumn("ratings", df.temp[2])\
    .drop("temp")

movie_rating_final = df2.groupBy("movie_id").agg(avg(df2.ratings).alias("avg_ratings"), count("*").alias("count"))\
    .filter("count > 100")

move_detail = spark.read\
    .format("text")\
    .option("path", "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/"
                    "Week11-Apache Spark - Structured API - Part1/Datasets/"
                    "movies-201019-002101.dat")\
    .load()

df4 = move_detail.selectExpr("split(value, '::') as temp1")

move_detail_final = df4.withColumn("movie_id", df4.temp1[0])\
    .withColumn("movie_name", df4.temp1[1])\
    .drop("temp1")

join_condition = movie_rating_final.movie_id == move_detail_final.movie_id

df5 = movie_rating_final.join(move_detail_final, join_condition, "inner").drop(move_detail_final.movie_id)\
    .where("avg_ratings > 4")

df5.orderBy(col("avg_ratings").desc()).show()

temp= input("enter any key")