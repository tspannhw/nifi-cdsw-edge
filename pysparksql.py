from __future__ import print_function
import pandas as pd
import sys, re
from operator import add
from pyspark.sql import SparkSession
pd.options.display.html.table_schema = True
  
spark = SparkSession\
  .builder\
  .appName("Gluon")\
  .getOrCreate()

# Access the parquet  
# /tmp/sensors/*.parquet
sensor = spark.read.parquet("/tmp/gluon/*.parquet")
data = sensor.toPandas()
pd.DataFrame(data)

status = spark.read.parquet("/tmp/status/*.parquet")
data2 = status.toPandas()
pd.DataFrame(data2)

tf = spark.read.parquet("/tmp/minifitensorflowp/*.parquet")
data3 = tf.toPandas()
pd.DataFrame(data3)

movit = spark.read.parquet("/tmp/movidiussensep/*.parquet")
data4 = movit.toPandas()
pd.DataFrame(data4)

# /tmp/coral

coral = spark.read.parquet("/tmp/coral/*.parquet")
data5 = coral.toPandas()
pd.DataFrame(data5)


spark.stop()
