from pyspark import SparkContext
#This is an example of groupByKey function. groupByKey return (Key, [list of values])
sc = SparkContext("local[*]", "bigLogCount")
#Set the error level as below.
#sc.setLogLevel("ERROR")
path = "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week10 - Apache Spark - In Depth/Datasets/" \
       "bigLog.txt"

input_rdd = sc.textFile(path)
big_log_tuple = input_rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))
grouped_data = big_log_tuple.groupByKey().map(lambda x: (x[0],len(x[1]))).collect()
for x in grouped_data:
       print(x)