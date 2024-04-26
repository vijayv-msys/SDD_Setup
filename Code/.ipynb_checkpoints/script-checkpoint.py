
import uuid
from kafka.admin import KafkaAdminClient, NewTopic
import subprocess
from time import sleep
# Execute the shell command
#kill past kafka server
command = "sudo kill -9 $(sudo lsof -t -i:9092)"
subprocess.run(command, shell=True)
#kill past spark server
command = "sudo kill -9 $(sudo lsof -t -i:4040)"
subprocess.run(command, shell=True)
command = "sudo kill -9 $(sudo lsof -t -i:9083)"
subprocess.run(command, shell=True)
# Generate a random Kafka cluster ID
kafka_cluster_id = str(uuid.uuid4())

# Execute 'kafka-storage.sh random-uuid' command to generate Kafka cluster ID
storage_command = ["/home/ubuntu/kafka/bin/kafka-storage.sh", "random-uuid"]
process = subprocess.Popen(storage_command, stdout=subprocess.PIPE)
output, _ = process.communicate()
kafka_cluster_id = output.decode('utf-8').strip()

# Execute 'kafka-storage.sh format' command with Kafka cluster ID and server properties
format_command = ["/home/ubuntu/kafka/bin/kafka-storage.sh", "format", "-t", kafka_cluster_id, "-c", "/home/ubuntu/kafka/config/kraft/server.properties"]
subprocess.run(format_command)
#sleep(5)# Execute 'kafka-server-start.sh' command with server properties
start_command = ["/home/ubuntu/kafka/bin/kafka-server-start.sh", "/home/ubuntu/kafka/config/kraft/server.properties"]
subprocess.Popen(start_command)
sleep(15)
try:
    subprocess.run('hdfs dfsadmin -safemode leave', shell=True, check=True)
except:
    print('safe')
    pass
subprocess.run('start-dfs.sh', shell=True, check=True)
sleep(5)
subprocess.run('hive --service metastore', shell=True, check=True)
sleep(5)
print('hive Done')

"""try:
    

    # Create a new topic
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    topic = NewTopic(name="test", num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])

    print("Kafka cluster and topic creation completed successfully.")

except Exception as e:
    print("An error occurred during Kafka cluster and topic creation:")
    print(str(e))
sleep(10)"""