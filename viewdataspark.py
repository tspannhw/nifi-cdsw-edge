# !hdfs dfs -ls /tmp

from __future__ import print_function
import pandas as pd
import sys, re
from operator import add
from pyspark.sql import SparkSession
pd.options.display.html.table_schema = True
  
spark = SparkSession\
  .builder\
  .appName("Sensors")\
  .getOrCreate()

# Access the parquet  
sensor = spark.read.parquet("/tmp/sensors/*.parquet")
# show contents
sensor.show()    
# query
sensor.select(sensor['bme680_humidity'], sensor['bme680_tempf'], sensor['lsm303d_magnetometer']).show()

sensor.printSchema()
sensor.count()

data = sensor.toPandas()
pd.DataFrame(data)

spark.stop()

# https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_import_data.html#access_hdfs
# To Load Data Created By niFi
# !hdfs dfs -mkdir /tmp/sensors
# !hdfs dfs -put parquet/*.parquet /tmp/sensors
# 
# https://github.com/cloudera/cdsw-training/blob/master/example.py

# maybe make a Spark SQL job, call it rest
# https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_rest_apis.html#cdsw_api
