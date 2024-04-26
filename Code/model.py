import csv
import math
import os
import re
import glob
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from kafka import KafkaConsumer
from json import loads
#import config
import json
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType,DoubleType
from pyspark.sql.functions import to_timestamp
import pandas as pd
from time import sleep
from pyspark.sql.functions import monotonically_increasing_id,col
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from utlis import model_train

print('Starting Modelling..')
# Create SparkSession
spark = SparkSession.builder.appName("processSmartBatch").enableHiveSupport().getOrCreate()
# Specify the table name
table_name = "lenovo_table"
query = f"SELECT * FROM {table_name}"

df = spark.sql(query)
#spark.sql(f"DROP TABLE IF EXISTS {table_name}")
#df = spark.read.format("parquet").load(table_location)
# Save DataFrame as CSV
print(df.count())
#print(df.show(10))

result = model_train(df)
result.show(10)
