#!/usr/bin/python
# -*- coding: utf-8 -*-
###
### This file parses timetable pdfs converted to text files 
### and builds a graph of the stops
###
### It then trains a power iteration clustering algorithm on the graph
### and writes the results to csv

import sys
from pyspark import SparkContext
from pyspark import SparkConf
from os import listdir
import re
import requests
import codecs
import tarfile
from pyspark.mllib.clustering import *

if len(sys.argv) <= 1:
    master = 'local'
else:
    master = sys.argv[1]

###
### Start up a Spark Context - taking care of everything clustery
###
### To be able to run code on local machine, we however make it local
###
conf = SparkConf()
conf.set("spark.eventLog.enabled", True)
conf.setMaster(master)
conf.setAppName("Vasttrafik stop clustering")
conf.set("spark.eventLog.dir", "tmp/")
conf.set("spark.executor.memory", "10g")


sc = SparkContext(conf = conf)

### Read up all stops from converted pdf-files

mytarfile = "timetables.tar.gz"
dir = "data/"

tarfile.open(mytarfile, 'r:gz').extractall()

### Read all testfiles in the given data directory
###
### In a cluster scenario files could be stored in a distributed
### filesystem such HDFS instead, in which case this code
### would be a little bit different.
timetables = filter(lambda file : '.txt' in file, listdir(dir))


def timetableRDD(file):
    regexp = u"^.* (?P<int>[0-9]+) +(?P<string>[\w ]+)$"
    ### sc.textfile makes an RDD[String] object and we then apply regexp to it to find the interesting rows
    ### The 1 in the argument list to textFile ensures that we only make one partition of the file
    timetable_regex = sc.textFile(dir + file, 1).map(lambda row: re.match(regexp, row, re.UNICODE))
    ### We filter out failed matches (None) and then maps the result to a key-value RDD of the form RDD[(Time, Stop)]
    timetable_time_stop = timetable_regex.filter(lambda m: m is not None).map(lambda m: [m.group('int'), m.group('string')])
    ### Again a filter to get rid of some more faulty rows.
    timetable_time_stop_filtered = timetable_time_stop.filter(lambda x: x[0].isdigit() and re.match(r"^[A-Z].*", x[1][0], re.UNICODE) != None)
    ### A glom makes an array of the RDDs key-values for every partition (similary to groupByKey but on partition level)
    timetable_glom = timetable_time_stop_filtered.glom()
    ### Return an RDD of the form RDD[((Stop1, Stop2), timediff)] where timediff is time to take between stops
    return timetable_glom.flatMap(lambda x: map(lambda i : [(x[i+1][1].lower(), x[i][1].lower()), (int(x[i + 1][0]) - int(x[i][0]), 1)], xrange(len(x) -1))).filter(lambda x: x[1][0] > 0)


### Make a single RDD from all the file RDDs parsed by timetableRDD function
timetable_data_union = sc.union(map(lambda x: timetableRDD(x), timetables))

### Here we repartition to make 100 partitions (instead of the ~ 5k we have)
timetable_data_union_repartitioned = timetable_data_union.repartition(100)

### Since many stops and trams go between directly between same stops, take minimum time between them to get geographical distance
### We also cache the result so that we can reuse it without having to read everything from file again
timetable_reduction = timetable_data_union_repartitioned.reduceByKey(lambda x, y: (min(x[0], y[0]), x[1] + y[1])).cache()

### Get list of all unique stops, and give them a unique index
### collect() command sends the result to the "driver" node and must hence fit in its memory
unique_stops = sc.union([timetable_reduction.map(lambda row : row[0][1]), timetable_reduction.map(lambda row : row[0][0])]).distinct().zipWithIndex().collect()

### We make maps to transform stop name to index and back and send the results to the workers
stops_to_index = sc.broadcast(dict((x, y) for x, y in unique_stops))
stops_from_index = sc.broadcast(dict((y, x) for x, y in unique_stops))

### Now we are ready to calculate affinity matrix!
### Use 1/time**2 as distance function
### Since affinity matrix is symmetric we filter out lower triangular part.
affinity_matrix = timetable_reduction.map(lambda x: [stops_to_index.value[x[0][0]], stops_to_index.value[x[0][1]], 1.0/pow(x[1][0], 2)]).filter(lambda x: x[0] < x[1]).cache()

### Train PIC
model = PowerIterationClustering.train(affinity_matrix, 25, 10)

### broadcast model assignments
index_to_cluster = sc.broadcast(dict((x, y) for x, y in model.assignments().map(lambda ass: (ass.id, ass.cluster)).collect()))

### Visualise in R - So collect result 

edges = affinity_matrix.map(lambda row: [stops_from_index.value[row[0]], stops_from_index.value[row[1]], index_to_cluster.value[row[0]], index_to_cluster.value[row[1]], row[2]]).collect()
f = codecs.open('out.txt', mode="w", encoding="utf-8")
[f.write(x[0] + u";" + x[1] + u";" + str(x[2]) + u';' + str(x[3]) + u';' + str(x[4]) + u"\n") for x in edges]


### Clean out cached RDDs
timetable_reduction.unpersist()
affinity_matrix.unpersist()
