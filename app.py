import numpy as np
from flask import Flask, request, render_template,jsonify,session
import joblib as joblib
import os
import pandas as pd
import json
import random
from werkzeug.utils import secure_filename
import psycopg2
import streamlit as st
import multiprocessing
import subprocess
import time
import pickle



app = Flask(__name__)


#*** Flask configuration
 
# Define folder to save uploaded files to process further
UPLOAD_FOLDER = os.path.join('staticFile')
 
# Define allowed files (for this example I want only csv file)
ALLOWED_EXTENSIONS = {'csv'}
 
app = Flask(__name__, template_folder='templates', static_folder='staticFile')
# Configure upload file path flask
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
 
# Define secret key to enable session
app.secret_key = 'This is your secret key to utilize session in Flask'

def terminate_subprocess():
    # Terminate any running subprocess with the same file name
    try:
        subprocess.check_output(["pkill", "-f", "run.py"])
    except subprocess.CalledProcessError:
        pass


# Front End
@app.route('/')
def index():
    return render_template('msys_home.html')


@app.route('/ssd_failure_prediction',  methods=("POST", "GET"))
def ssd_failure_prediction():
    if request.method == 'POST':
        time.sleep(5)
        
        terminate_subprocess()
       # Get the current script's directory
        current_directory = os.path.dirname(os.path.abspath(__file__))

        # Navigate one folder back
        parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
        print(parent_directory,'parent_directory')

        # Construct the full path to the file
        file_path = os.path.join(parent_directory,"SSD_Setup", "Code", "run.py")
        print('Code',file_path)

        
        process = subprocess.Popen(['python3', file_path])
        time.sleep(10)
        
        
 
        return render_template('hdd_failure_prediction.html', prediction=True)
    else:
        return render_template('hdd_failure_prediction.html', prediction=False)

@app.route('/ssd_failure_prediction_result',  methods=("POST", "GET"))
def ssd_failure_prediction_result():
    return render_template('hdd_failure_prediction_result.html')

# PostgreSQL configuration
DB_HOST = 'localhost'
DB_NAME = 'results'
DB_USER = 'postgres'
DB_PASSWORD =1234
def fetch_data_from_database():
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM "Result"')
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data
@app.route('/database', methods=["GET"])
def database():
    data = fetch_data_from_database()
    print(jsonify(data))
    return jsonify(data)





if __name__ == '__main__':
    # app.run()
    app.run(host = '0.0.0.0', port = 8080)
