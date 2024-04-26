import subprocess
from time import sleep
import subprocess
import os

 # Get the current script's directory
current_directory = os.path.dirname(os.path.abspath(__file__))

# Navigate one folder back
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

# Construct the full path to the file
file_path1 = os.path.join(parent_directory ,"Code", "script.py")
file_path2 = os.path.join(parent_directory,"Code", "consumer.py")
file_path3 = os.path.join(parent_directory,"Code", "producer.py")
# Run script.py
script_process = subprocess.Popen(['python3', file_path1])
sleep(50)
# Run consumer.py
consumer_process = subprocess.Popen(['python3', file_path2])
sleep(40)

# Run producer.py
producer_process = subprocess.Popen(['python3', file_path3])

# # Run script.py
# script_process = subprocess.Popen(['python3', 'Code/script.py'])
# sleep(50)
# # Run consumer.py
# consumer_process = subprocess.Popen(['python3', 'Code/consumer.py'])
# sleep(40)

# # Run producer.py
# producer_process = subprocess.Popen(['python3', 'Code/producer.py'])

# Wait for producer.py to complete
producer_process.wait()

sleep(20)
# Terminate script.py and consumer.py
consumer_process.terminate()
script_process.terminate()


# Wait for script.py and consumer.py to terminate
script_process.wait()
consumer_process.wait()
sleep(2)
subprocess.run('stop-dfs.sh', shell=True, check=True)
print("All processes completed.")
