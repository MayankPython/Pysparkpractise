from pyspark import SparkContext

sc = SparkContext("local[*]", "wordCount")
path = "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week09 - Apache Spark/Datasets/" \
       "-201125-161348.dataset1"
input = sc.textFile(path)

def update_tuple (input_word):
    if int(input_word[1]) > 18:
        return (input_word[0],input_word[1],input_word[2], "Y")
    else:
        return (input_word[0],input_word[1],input_word[2], "N")

words = input.map(lambda x: x.split(","))
rdd1 = words.map(update_tuple)
result = rdd1.collect()
for x in result:
    print(x)
