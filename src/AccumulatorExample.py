from pyspark import SparkContext

def blank_line_counters(line):
    if len(line) == 0:
        blank_line_count.add(1)

sc = SparkContext("local[*]", "AccumulatorExample")
path = "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week10 - Apache Spark - In Depth/Datasets/" \
       "simple_file.txt"

input_rdd = sc.textFile(path)
blank_line_count = sc.accumulator(0)

input_rdd.foreach(blank_line_counters)
print(blank_line_count)