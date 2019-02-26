!from __future__ import print_function
import pandas as pd
import sys, re
import uuid
import os
import json
from operator import add
from pyspark.sql import SparkSession
pd.options.display.html.table_schema = True

# article:  https://community.hortonworks.com/articles/239961/using-cloudera-data-science-workbench-with-apache.html

spark = SparkSession\
  .builder\
  .appName("Gluon Results")\
  .getOrCreate()

filename = '{0}'.format( uuid.uuid4())

default_value = 'NA'

#     "uniqueid" : "cca66150-a8a2-4648-9962-c4ef895f33f2",
#    "memory" : 26.2,
#    "class1" : "person",
#    "cpu" : 0.8,
#    "end" : "1551193140.8637178",
#    "host" : "gluoncv-apache-mxnet-29-77-699fcbddb8-kq8wb",
#    "pct1" : "",
#    "shape" : "(1, 3, 512, 683)",
#    "systemtime" : "02/26/2019 14:59:00",
#    "te" : "1.99894118309021"

row = {}
row['host'] = os.getenv('host', default_value)
row['end'] = os.getenv('end', default_value)
row['te'] = os.getenv('te', default_value)
row['memory'] = os.getenv('memory', default_value)
row['systemtime'] = os.getenv('systemtime', default_value)
row['uniqueid'] = os.getenv('uniqueid', default_value)
row['class1'] = os.getenv('class1', default_value)
row['cpu'] = os.getenv('cpu', default_value)
row['pct1'] = os.getenv('pct1', default_value)
row['shape'] = os.getenv('shape', default_value)

json_string = json.dumps(row)
            
print(json_string)
df = spark.read.json(spark.sparkContext.parallelize([json_string]))
df.show(truncate=False)

# !hdfs dfs -mkdir -p /tmp/gluon
# !hdfs dfs -chmod -R 777 /tmp/gluon

# Store the parquet  
df.write.parquet("/tmp/gluon/" + filename + ".parquet")

# {"class1": "cat", "pct1": "98.15670800000001", "host": "gluoncv-apache-mxnet-29-49-67dfdf4c86-vcpvr", "shape": "(1, 3, 566, 512)", "end": "1549671127.877511", "te": "10.178656578063965", "systemtime": "02/09/2019 00:12:07", "cpu": 17.0, "memory": 12.8}
# run in pyspark / python 2 session

# 
# !hdfs dfs -mkdir /tmp/images
# !hdfs dfs -chmod -R 777 /tmp/images

spark.stop()
