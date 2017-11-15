from __future__ import print_function
import sys, json
from pyspark import SparkContext

sc = SparkContext(appName="tweetTime")

file=sc.textFile("hdfs://hadoop2-0-0/data/twitter").filter(lambda line: "PrezOno" in json.loads(line)["user"]["screen_name"])
 
tweetUserMap = file.map(lambda line : (((json.loads(line)['created_at']).split()[3]).split(":")[0],1)).reduceByKey(lambda a, b: a+b)
output = sc.parallelize(tweetUserMap.take(25))
output.saveAsTextFile("twthr")