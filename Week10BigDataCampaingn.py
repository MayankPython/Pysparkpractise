from pyspark import SparkContext

def loadboringwords():
    boring_words = set()
    #read the file
    boring_file = open("C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week10 - Apache Spark - In Depth/"
                       "Datasets/boringwords.txt", "r")
    for x in boring_file:
        boring_words.add(x.rstrip("\n"))
    return boring_words

sc = SparkContext("local[*]", "BigDataCampaing")
path = "C:/Users/mayan/OneDrive/Desktop/Big Data/Download materials/Week10 - Apache Spark - In Depth/Datasets/" \
       "bigdatacampaigndata-201014-183159.csv"

input = sc.textFile(path)
name_set = sc.broadcast(loadboringwords())

work_tuple = input.map( lambda x: (float(x.split(",")[10]), x.split(",")[0]))
word_tuple = work_tuple.flatMapValues(lambda x: x.split(" "))
final_tuple = word_tuple.map(lambda x:(x[1].lower(),x[0]))
filtered_output = final_tuple.filter(lambda x: x[0] not in name_set.value)
final_output = filtered_output.reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False).collect()
for x in final_output:
    print(x)
#To save the output on text file.
#final_output = filtered_output.reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
#final_output.saveAsTextFile("C:/Users/mayan/OneDrive/Desktop/Big Data/Program output")
