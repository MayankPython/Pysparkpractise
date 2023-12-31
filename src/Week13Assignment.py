from pyspark import SparkContext

sc = SparkContext("CloudTest")
path = "/home/hadoop/bigLogLatest.txt"
"""
ERROR: Thu Jun 04 10:37:51 BST 2015
WARN: Sun Nov 06 10:37:51 GMT 2016
WARN: Mon Aug 29 10:37:51 BST 2016
ERROR: Thu Dec 10 10:37:51 GMT 2015
ERROR: Fri Dec 26 10:37:51 GMT 2014
ERROR: Thu Feb 02 10:37:51 GMT 2017
WARN: Fri Oct 17 10:37:51 BST 2014
ERROR: Wed Jul 01 10:37:51 BST 2015
"""
rdd1 = sc.textFile(path)
rdd2 = rdd1.map(lambda x: (x.split(":")[0], x.split(":")[1]))
rdd3 = rdd2.groupByKey()
rdd4 = rdd3.map(lambda x:(x[0], len(x[1])))
rdd4.collect()


