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
data = sensor.toPandas()
pd.DataFrame(data)
spark.stop()
