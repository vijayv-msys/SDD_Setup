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
import pickle
import numpy as np


script_dir = os.path.dirname(__file__)
model_file = "SMOTE_XGB_model.pkl"
model_path = os.path.join(script_dir, model_file)


def model_train(data,table_count,spark):
    data=data.toPandas()
    selected_features = ['R_ECC_RECOVERED',
 'R_READ_ERROR_RATE',
 'R_COMMAND_TIMEOUT',
 'N_WEAR_LEVELING_COUNT',
 'R_UNUSED_BLOCK_COUNT',
 'R_CRC_ERROR',
 'R_ERASE_FAIL_COUNT',
 'N_UNUSED_BLOCK_COUNT',
 'R_PROGRAM_FAIL_COUNT',
 'R_UNCORR_SECTOR_COUNT',
 'N_PROGRAM_FAIL_COUNT']
    x=data[['R_ECC_RECOVERED',
 'R_READ_ERROR_RATE',
 'R_COMMAND_TIMEOUT',
 'N_WEAR_LEVELING_COUNT',
 'R_UNUSED_BLOCK_COUNT',
 'R_CRC_ERROR',
 'R_ERASE_FAIL_COUNT',
 'N_UNUSED_BLOCK_COUNT',
 'R_PROGRAM_FAIL_COUNT',
 'R_UNCORR_SECTOR_COUNT',
 'N_PROGRAM_FAIL_COUNT','ISFAULT']]
    #loaded_model = pickle.load(open("SMOTE_XGB_model.pkl", "rb"))
    loaded_model = pickle.load(open(model_path, "rb"))
    predictions = loaded_model.predict(x[selected_features])
    test_target = x['ISFAULT']
    test_target.reset_index(drop=True, inplace=True)
    test_target = test_target.replace({1:'Failed', 0:'Active'})
    predicted_target = pd.Series(predictions).replace({1:'predicted_failed', 0:'predicted_active'})
    xgb_pred_prob = loaded_model.predict_proba(x[selected_features])[:,1]
    active_prob=1-xgb_pred_prob
    output=pd.DataFrame(data=
                   {   'Actual':test_target.values, 
                       'Prediction':predicted_target.values,
                       'Active_probability':active_prob,
                       'Failure_probability':xgb_pred_prob
                   }
                   )
    final_df=pd.concat([x[selected_features], output], axis=1,join='outer')
    conditions = [
    (final_df['Failure_probability'] > 90),
    (final_df['Failure_probability'] > 75) & (final_df['Failure_probability'] <= 90),
    (final_df['Failure_probability'] > 60) & (final_df['Failure_probability'] <= 75),
    (final_df['Failure_probability'] > 45) & (final_df['Failure_probability'] <= 60),
    (final_df['Failure_probability'] > 30) & (final_df['Failure_probability'] <= 45),
    (final_df['Failure_probability'] > 15) & (final_df['Failure_probability'] <= 30),
    (final_df['Failure_probability'] <= 15)]

    choices = ['Critical', 'Very High', 'High', 'Moderate', 'Low', 'Very Low', 'Healthy']
    final_df['Chances_of_failure'] = np.select(conditions, choices, default=np.nan)

    
    #print(pandasDF)
    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://postgres:1234@localhost/results')



    
    table_name = 'results'+str(table_count)
    final_df.to_sql(table_name, engine, if_exists='replace', index=False)
    final_df.to_sql("Result", engine, if_exists='replace', index=False)
    try:
        os.remove('result_stream.csv')
    except:
        pass

    final_df.to_csv('result_stream.csv', index=False)
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

    

    return spark.createDataFrame(final_df)
