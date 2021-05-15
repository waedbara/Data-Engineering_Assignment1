import csv
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import pandas as pd
import os
import json

try:
    from faker import Faker
except:
    subprocess.check_call(['pip3', 'install', 'faker'])
try:
    from sqlalchemy import create_engine
except:
    subprocess.check_call(['pip3', 'install', 'sqlalchemy'])
try:
    import psycopg2
except:
    subprocess.check_call(['pip3', 'install', 'psycopg2'])

try:
    import pymongo
except:
    subprocess.check_call(['pip3', 'install', 'pymongo'])

# config varabiles
host = Variable.set("host", "postgres")
user = Variable.set("user", "airflow")
password = Variable.set("password", "airflow")
port = Variable.set("port", '5432')
database = Variable.set("database", 'postgres')
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def GenerateCSV():
    output = open('data.csv', 'w')
    fake = Faker()
    header = ['name', 'age', 'street', 'city', 'state', 'zip', 'lng', 'lat']
    mywriter = csv.writer(output)
    mywriter.writerow(header)
    for r in range(10):
        row = [fake.name(), fake.random_int(min=18, max=80, step=1),
               fake.street_address(), fake.city(), fake.state(),
               fake.zipcode(), fake.longitude(), fake.latitude()]
        print(row)
        mywriter.writerow(row)

    output.close()
    DF = pd.read_csv('data.csv')
    DF.to_csv(AIRFLOW_HOME + '/dags/dataframe.csv', index=False)


def SaveCsvToPostgres():
    host = Variable.get('host')
    user = Variable.get('user')
    password = Variable.get('password')
    port = Variable.get('port')
    database = Variable.get('database')
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database}')
    print("Airflow Database Tables :- ", engine.table_names())
    DF = pd.read_csv(AIRFLOW_HOME + '/dags/dataframe.csv')
    # push table
    DF.to_sql('users', engine, if_exists='replace', index=False)


def ConvertCsvToJson(ti):
    # read from postgres
    host = Variable.get('host')
    user = Variable.get('user')
    password = Variable.get('password')
    port = Variable.get('port')
    database = Variable.get('database')
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{database}')
    DF2 = pd.read_sql("SELECT * FROM users", engine)

    for i, r in DF2.iterrows():
        print(r['name'])

    DF2.to_json(AIRFLOW_HOME + '/dags/fromAirflow.json', orient='records')


def SaveJsonMongodb(ti):
    from pymongo import MongoClient
    client = MongoClient('mongo:27017',
                         username='root',
                         password='example')
    db = client['users']
    # Create Collection
    usersInfo = db.usersInfo
    with open(AIRFLOW_HOME + '/dags/fromAirflow.json') as f:
        users = json.load(f)
    # Push documents to collection
    for key in users:

        usersInfo.insert_one(key)


default_args = {
    'owner': 'waed',
    'start_date': dt.datetime(2020, 5, 15),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('DAG1',
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         catchup=False,
         ) as dag:
    GenerateCsvFile = PythonOperator(task_id='GenerateCSVFile',
                                     python_callable=GenerateCSV)

    SaveCsvToPostgres = PythonOperator(task_id='SaveCsvToPostgres',
                                       python_callable=SaveCsvToPostgres)
    ConvertCsvToJson = PythonOperator(task_id='ConvertCsvToJson',
                                      python_callable=ConvertCsvToJson)

    SaveJsonMongodb = PythonOperator(task_id='SaveJsonMongodb',
                                     python_callable=SaveJsonMongodb)

GenerateCsvFile >> SaveCsvToPostgres >> ConvertCsvToJson >> SaveJsonMongodb
