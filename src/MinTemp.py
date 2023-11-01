from pyspark import SparkContext

sc = SparkContext("local[*]", "wordCount")
path = "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week09 - Apache Spark/Datasets/" \
       "tempdata-201125-161348.csv"
input = sc.textFile(path)

word_tuple = input.map(lambda x: (x.split(",")[0], int(x.split(",")[3])))
reduced_word = word_tuple.reduceByKey(lambda x,y: x if (x < y) else y)

final_output = reduced_word.collect()
print(final_output)
for x in final_output:
       print(x)