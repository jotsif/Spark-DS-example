# Spark-DS-example
Illustration of how to use Apache Spark to analyse raw data, for MLSD meetup GBG 2015

The example is an attempt to do unsupervised graph clustering of Västtrafik stops using timetable pdfs converted to txt files (stored in timetables.tar.gz). The model is a power iteration graph clustering algorithm. 

![alt tag](https://github.com/jotsif/Spark-DS-example/blob/master/plots/graph.png)

# Running

1) Download apache spark binary suitable for your OS, and R if not installed either. 
```brew install apache-spark``` for Mac OS X for example.

2) Run the following code to execute the spark python script from a terminal:
```spark-submit --driver-memory=6G py/readPdfs.py```

The code automatically extracts the timetable files so no need to do it manually.

This might take a while, since it parses more than 5k files, parses them and trains a model. 

3) Visualise the result and compare PIC with Louvain graph clustering done in R.

```R -f R/plotgraph.R```

Plots saved in plots/ directory.
