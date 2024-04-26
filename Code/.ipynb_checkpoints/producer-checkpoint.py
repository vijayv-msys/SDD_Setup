from kafka import KafkaProducer
from kafka.errors import kafka_errors
from time import sleep
from json import dumps
import pandas as pd
import sys
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import datetime
import os

# Get the current time before the code block
start_time = datetime.datetime.now()

# Your code block
# Your code goes here



# Configure Kafka admin client
bootstrap_servers = 'localhost:9092'
topic = 'Stream'

# Create Kafka admin client
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Delete the topic
admin_client.delete_topics(topics=[topic])

# Close the admin client
admin_client.close()

topic = 'Stream'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: dumps(x).encode('utf-8')
                        ,key_serializer= lambda x: dumps(x).encode('utf-8'))


 # Get the current script's directory
current_directory = os.path.dirname(os.path.abspath(__file__))

# Navigate one folder back
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

# Construct the full path to the file
file_path = os.path.join(parent_directory, "Code", "candidates.csv")

def send():
    
    df = pd.read_csv(file_path)    
    # Send each row to Kafka topic
    df.drop("Unnamed: 0",inplace=True,axis=1)
    counter = 0  # Counter variable to track the number of rows processed
    #print(df.shape)
    for index, row in df.iloc[:,:].iterrows():
        #print(index)
        #if counter >= 25:
            #print('done producer')
            #break # Exit the loop after processing 100 rows
    
        # Serialize the key
        key = {'id': index}
    
        # Serialize the value
        value = row.to_dict()
    
        # Send the serialized key-value pair to Kafka topic
        producer.send(topic, key=key, value=value)
        # print('send')
    
        counter += 1  # Increment the counter
        sleep(1)
        producer.flush()
    
        # Sleep for a moment to simulate real-time streaming
        # Close the Kafka producer
    producer.close()
send()



# Get the current time after the code block
end_time = datetime.datetime.now()


# Calculate the runtime in seconds
runtime_seconds = (end_time - start_time).total_seconds()

# Convert the runtime to minutes and seconds
runtime_minutes = int(runtime_seconds // 60)
runtime_seconds %= 60

# Print the total runtime in minutes and seconds format
print(f"Total Run Time - {runtime_minutes} minutes {runtime_seconds:.2f} seconds")