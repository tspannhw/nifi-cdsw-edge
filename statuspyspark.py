# To Load Data Created By niFi
# !hdfs dfs -mkdir /tmp/status
# !hdfs dfs -put status/*.parquet /tmp/status
# !hdfs dfs -ls /tmp/status

from __future__ import print_function
import pandas as pd
import sys, re
from operator import add
from pyspark.sql import SparkSession
pd.options.display.html.table_schema = True
  
spark = SparkSession\
  .builder\
  .appName("Status")\
  .getOrCreate()

# Access the parquet  
sensor = spark.read.parquet("/tmp/status/*.parquet")
# show contents
sensor.show()    
# query
# sensor.select(sensor['bme680_humidity'], sensor['bme680_tempf'], sensor['lsm303d_magnetometer']).show()

sensor.printSchema()
sensor.count()

data = sensor.toPandas()
pd.DataFrame(data)

spark.stop()
1.5.0.842775 (27c8f2d)
