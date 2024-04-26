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
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType,TimestampType,DoubleType
from pyspark.sql.functions import to_timestamp,lit
import pandas as pd
from utlis import model_train

# Create SparkSession
spark = SparkSession.builder.appName("processSmartBatch").enableHiveSupport().getOrCreate()

for i in range(1,50):
    try:
        t = 'tableNumber'+str(i)
        #print(t)
        spark.sql(f"DROP TABLE {t}")
        print("Table Deleted -",t)
    except:
        break  
for i in range(1,50):
    try:
        t = 'tableNumber_'+str(i)
        #print(t)
        spark.sql(f"DROP TABLE {t}")
        print("Table Deleted -",t)
    except:
        break  
table_count=1
try:
    spark.sql(f"DROP TABLE table_name")
except:
    pass


# Function to load data into Hive table row by row
def load_data(data):
    table_name = 'table'
    global table_count
    table_name = table_name+str(table_count)
    #print(table_name)

    # Create a DataFrame with a single row of data
    df = spark.createDataFrame([tuple(data.values())], list(data.keys()))

    # Check if the table exists
    if spark.catalog.tableExists(table_name):
        # Append the data to the existing table
        df.write.mode("append").insertInto(table_name)
        #print("Table append")
    else:
        # Create the table and insert the data
        df.write.saveAsTable(table_name)
        #print('New Table Created',table_name)
    
    # Get the row count of the table
    row_count = spark.sql(f"SELECT COUNT(*) AS count FROM {table_name}")
    count = row_count.first()["count"]
    print("Number of rows in the table:", count,"Table -",table_name)
    
    if count % 100 == 0 or count==100:
        query = f"SELECT * FROM {table_name}"
        data = spark.sql(query)
        result = model_train(data,table_count,spark)
        result.show(10)
        table_count+=1
        df = spark.createDataFrame([(table_name,)], StringType())
        # Check if the table exists
        if spark.catalog.tableExists('table_names'):
        # Append the data to the existing table
            df.write.mode("append").insertInto('table_names')
        else:
        # Create the table and insert the data
            df.write.saveAsTable('table_names')
            print('New Table Created for Table names')
        #spark.sql(f"TRUNCATE TABLE {table_name}")
        #print(spark.sql(f"SELECT * FROM table_name"))

# Kafka consumer settings
topic = 'Stream'
consumer = KafkaConsumer(
    topic, group_id='1',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: loads(x),
    key_deserializer=lambda x: loads(x),
    auto_offset_reset='earliest'
)


# Consume messages from Kafka
for message in consumer:
    data = message.value
    # print(message.key)
    
    # Load data into the corresponding table
    load_data(data)  
    #consumer.commit()
    

    
spark.stop()
