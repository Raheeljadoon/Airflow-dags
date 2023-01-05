import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import datetime
import requests
from bs4 import BeautifulSoup
from airflow.utils.dates import days_ago
# from python_function import  name_getter
import requests
from bs4 import BeautifulSoup
# from names_dag import name_list
import json
import psycopg2
name_list = []

def name_getter(gender_perc, list_):
    URL = "https://api.name-fake.com/english-united-state/"
        
    male_request = requests.post(URL, data={'perc':gender_perc})
    soup = BeautifulSoup(male_request.content, 'html.parser')
    name = soup.find('div', attrs= {'id': 'copy1'})

    list_.append({'name': name.text})
    return list_


def get_logs(str):
    print("************************************")
    print(str)
    print("************************************")
    return "yes"

def get_fake_name():


    name_list.clear()
      
    for _ in range(10):
        name_getter(0, name_list)
        name_getter(100, name_list)
    return  name_list

def get_gender(**context):
    context = context['task_instance']
    
    URl = "https://api.genderize.io/"
    main_list = context.xcom_pull(task_ids='get_name')
   
    # get_logs(main_list)
    for each in main_list:
        gender_ = requests.get(URl, params={'name':each['name']})
        each['gender'] = json.loads(gender_.text)['gender'] \
                    if json.loads(gender_.text)['gender'] != None else 'NA'
    return main_list

def get_country(**context):
    URL = "https://api.nationalize.io"
    context = context['task_instance']
    main_list = context.xcom_pull(task_ids='get_gender')
    for each in main_list:
        nationality = requests.get(URL, params={'name':each['name']})
        try:
            each['country'] = json.loads(nationality.text)['country'][0]['country_id']
        except:
            each['country'] = "NA" 
    return main_list

def get_age(**context):
    URL = "https://api.agify.io"
    # main_list = context['params']
    context = context['task_instance']
    main_list = context.xcom_pull(task_ids='get_national')
    for each in main_list:
        age_ = requests.get(URL, params={'name':each['name']})
        each['age'] = json.loads(age_.text)['age'] \
            if json.loads(age_.text)['age'] != None else 0
    print(main_list)
    return main_list

def insert_data(**context):
    try:
        context = context['task_instance']
        main_list = context.xcom_pull(task_ids='get_age')
        connection = psycopg2.connect(user="people_db",
                                    password="people_db",
                                    host="host.docker.internal",
                                    port="5432",
                                    database="people_db")
        cursor = connection.cursor()
        print(connection.get_dsn_parameters(), "\n")
        for each in main_list:
        
            cursor.execute("Insert into people_data_management_record (name,gender,age,nationality) \
                             values (%s, %s, %s, %s)", (each['name'], each['gender'], each['age'],each['country']))
        connection.commit()
    except Exception as er:
        print(er)
    finally:
        if (connection):
            cursor.close()
            connection.close()
        print("PostgreSQL connection is closed")


""" ------- dags configuration and execution starts from here --------"""

default_args = {
    'owner': 'raheel_airflow',
    # 'retries': 1,
    'start_date': days_ago(1),
    # 'retry_delay': timedelta(minutes=2)
}
dag_python = DAG(
    dag_id = 'people_db',
    default_args = default_args,
    schedule_interval = '0 1 * * *',
    description = 'Getting fake name from different API',
    start_date=datetime.datetime(2022,12,30,7),

)

get_name_ = PythonOperator(task_id='get_name', python_callable=get_fake_name, dag=dag_python)
get_gender_ = PythonOperator(task_id='get_gender', python_callable=get_gender,params={'result': get_name_}, dag=dag_python)
get_nationality_ = PythonOperator(task_id='get_national', python_callable=get_country,params={'na':get_gender_}, dag=dag_python)
get_age_ = PythonOperator(task_id='get_age', python_callable=get_age,params={'age': get_nationality_}, dag=dag_python)
insert_data_ = PythonOperator(task_id='insert_record', python_callable=insert_data, params={'data': get_age_}, dag=dag_python)

get_name_ >> get_gender_ >> get_nationality_ >> get_age_ >> insert_data_
if __name__ == "__main__":
    dag_python.cli()                                                           