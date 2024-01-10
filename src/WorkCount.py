from pyspark import SparkContext

sc = SparkContext("local[*]", "wordCount")
path = "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week09 - Apache Spark/Datasets/" \
       "search_data.txt"
input = sc.textFile(path)
word = input.flatMap(lambda x: x.split(" "))
word_count = word.map(lambda x: x.lower())
word_tuple = word_count.map(lambda x: (x, 1))
word_total = word_tuple.reduceByKey(lambda x,y: x+y)
word_sorted = word_total.sortBy(lambda x: x[1], False)
final_output = word_sorted.collect()
for x in final_output:
       print(x)