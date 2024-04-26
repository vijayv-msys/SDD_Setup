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
import pandas as pd
from sqlalchemy import create_engine
from time import sleep



def model_train(data,table_count,spark):
    #print(data.show(2))
    #print('count -',data.count())
    #sleep(5)
    selected_features = ['capacity_bytes',
 'smart_1_normalized',
 'smart_2_normalized',
 'smart_2_raw',
 'smart_3_normalized',
 'smart_3_raw',
 'smart_4_raw',
 'smart_5_normalized',
 'smart_5_raw',
 'smart_7_normalized',
 'smart_8_normalized',
 'smart_8_raw',
 'smart_9_normalized',
 'smart_9_raw',
 'smart_12_raw',
 'smart_18_normalized',
 'smart_22_normalized',
 'smart_22_raw',
 'smart_23_normalized',
 'smart_24_normalized',
 'smart_183_normalized',
 'smart_184_normalized',
 'smart_187_normalized',
 'smart_187_raw',
 'smart_188_normalized']
    x=data['capacity_bytes',
 'smart_1_normalized',
 'smart_2_normalized',
 'smart_2_raw',
 'smart_3_normalized',
 'smart_3_raw',
 'smart_4_raw',
 'smart_5_normalized',
 'smart_5_raw',
 'smart_7_normalized',
 'smart_8_normalized',
 'smart_8_raw',
 'smart_9_normalized',
 'smart_9_raw',
 'smart_12_raw',
 'smart_18_normalized',
 'smart_22_normalized',
 'smart_22_raw',
 'smart_23_normalized',
 'smart_24_normalized',
 'smart_183_normalized',
 'smart_184_normalized',
 'smart_187_normalized',
 'smart_187_raw',
 'smart_188_normalized','failure']   
    x = x.fillna('null').select([col(c).cast("integer").alias(c) for c in x.columns])
    x = x.fillna(0)
    
    assembler = VectorAssembler.load('file:///home/ubuntu/Code/assembled')
    assembled_data = assembler.transform(x)


    # Load the saved GBTClassifier model
    loaded_model = GBTClassificationModel.load("file:///home/ubuntu/Code/model")
    predictions = loaded_model.transform(assembled_data)

    # Display the predicted values
    predictions.select('failure', 'prediction').show()
    

    # Create an evaluator for multi-class classification
    try:
        evaluator = BinaryClassificationEvaluator(labelCol='failure')
        accuracy = evaluator.evaluate(predictions)
        #print('Accuracy:', accuracy)
    except:
        pass
    try:
        evaluator = MulticlassClassificationEvaluator(labelCol='failure', predictionCol='prediction', metricName='accuracy')
        accuracy = evaluator.evaluate(predictions)
        #print('Accuracy:', accuracy)
    except:
        pass

    # Calculate the accuracy
    accuracy = evaluator.evaluate(predictions)
    #print('Accuracy:', accuracy)

    # Create a confusion matrix
    confusion_matrix = predictions.groupBy('failure', 'prediction').count().orderBy('failure', 'prediction')
    confusion_matrix.show()
    result = data['serial_number','model','capacity_bytes', 'date']
    res1=predictions.select('failure', 'prediction','rawPrediction', 'probability')
    # Add a unique identifier column to each DataFrame
    df1 = result.withColumn('id', monotonically_increasing_id())
    df2 = res1.withColumn('id', monotonically_increasing_id())

    # Perform an inner join on the 'id' column
    concatenated_df = df1.join(df2, 'id', 'inner').drop('id')

    
    
    #final1_model_results = validation_serial_number_index.join(final1_index, "index", "inner")
    final1_model_results = concatenated_df.withColumnRenamed("rawPrediction", "Confidence_Level")
    #final1_model_results.show()

    # STEP-14 Download the predictions results
    pandasDF = final1_model_results.toPandas()
    pandasDF.failure.replace((0, 1), ('Active', 'fail'), inplace=True)
    pandasDF.prediction.replace((0, 1), ('Active', 'Predicted_to_be_fail'), inplace=True)
    pandasDF['Confidence_Level'] = pandasDF['Confidence_Level'].astype('str')
    pandasDF['probability'] = pandasDF['probability'].astype('str')
    Confidence_Level=pandasDF['Confidence_Level'].str.split(',', expand=True)
    pandasDF = pandasDF.join(Confidence_Level).drop(1, axis=1)
    pandasDF = pandasDF.drop('Confidence_Level', axis=1)
    pandasDF.rename(columns = {0:'Confidence_Level'}, inplace = True)
    probability1=pandasDF['probability'].str.split(',', expand=True)
    probability1.rename(columns = {0:'Active_Probability'}, inplace = True)
    probability1.rename(columns = {1:'Failure_Probability'}, inplace = True)
    pandasDF = pandasDF.join(probability1)
    pandasDF = pandasDF.drop(['probability'], axis=1)
    #pandasDF = pandasDF.drop(['index'],axis=1)
    pandasDF.rename(columns = {'serial_number':'Serial_Number'}, inplace = True)
    pandasDF.rename(columns = {'model':'Model'}, inplace = True)
    pandasDF.rename(columns = {'label':'Actaul'}, inplace = True)
    pandasDF.rename(columns = {'prediction':'Prediction'}, inplace = True)
    pandasDF=pandasDF.replace('\]','',regex=True).astype(str)
    pandasDF=pandasDF.replace('\[','',regex=True).astype(str)
    #pandasDF.sort_values(by=['Prediction'], inplace=True, ascending=False)
    #pandasDF.sort_values(by=['Active_Probability'], inplace=True)
    grafanadf = pandasDF
    #print(pandasDF)
    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://postgres:1234@localhost/results')



    
    table_name = 'results'+str(table_count)
    grafanadf.to_sql(table_name, engine, if_exists='replace', index=False)
    grafanadf.to_sql("Result", engine, if_exists='replace', index=False)
    try:
        os.remove('result_stream.csv')
    except:
        pass

    grafanadf.to_csv('result_stream.csv', index=False)
    #engine = create_engine('postgresql://postgres:1234@localhost/tables')
            # Establish a connection
    
    #print(df)
    """
    db_username = 'postgres'
    db_password = 'Vijay2590'
    db_host = 'postgresql.cf4zunotlmzs.eu-north-1.rds.amazonaws.com'
    db_port = '5432'
    db_name = 'test'
    connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    table_name = 'lenovo_result'  
    # dt.to_sql('employees', engine)

    grafanadf.to_sql(table_name, engine, if_exists='replace', index=False)"""

    
    #print('Grafana Done')

    # Optional: Close the database connection
    engine.dispose()

    

    return spark.createDataFrame(grafanadf)