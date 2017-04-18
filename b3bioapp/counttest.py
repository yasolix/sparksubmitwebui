"""wordcount.py"""
from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: counttest <file>", file=sys.stderr)
		exit(-1)

	sc = SparkContext("local", "Simple Word Count App")
	rdd=sc.textFile(sys.argv[1])
	wordcounts=rdd.flatMap(lambda l: l.split(' ')) \
    				.map(lambda w:(w,1)) \
    				.reduceByKey(lambda a,b:a+b) \
    				.map(lambda (a,b):(b,a)) \
    				.sortByKey(ascending=False)
	output = wordcounts.collect()
	for (count,word) in output:
		print("%s: %i" % (word,count))
	sc.stop()