# boaz_simple_etl.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import requests
from bs4 import BeautifulSoup
import csv

# 사용할 Operator Import
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def extract(link):
    f = requests.get(link)
    return f.text

def transform(text):
    soup = BeautifulSoup(text, 'html.parser')
    titles = []
    tmp = soup.select(".title")
    for title in tmp:
        titles.append(title.get_text())
    return titles

def load(titles):
    with open("job_search_result.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(titles)

def etl():
	link = "https://kr.indeed.com/jobs?q=airflow"
	text = extract(link)
	titles = transform(text)
	load(titles)
	

# DAG 설정
dag = DAG(
    dag_id="boaz_simple_etl",
    schedule_interval="@daily",
    start_date=datetime(2022,6,15), #원래 시작했어야 한 날짜 하지만 명시적으로 시작 안 해주면 dag는 시작 안 함
    default_args = { 
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
    }, #dag 밑에 속한 task에 적용되는 파라미터
    tags=["BOAZ"],  #팀별로, 태스크별로 tag 붙일 수 있음 (DAG 수백개 되면 하나하나 기억할 수 없음. tag 필수)
    catchup=False #start_date와 실제 시작일 사이에 실행 안 된 것을 한 번에 실행해주는 옵션 (모르겠으면 false…)
    ) 


# Operator 만들기
creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id="postgres_conn_id", # 웹UI에서 connection을 등록해줘야 함.
        sql='''
            CREATE TABLE IF NOT EXISTS job_search_result ( 
                title TEXT
            )
        ''',
        dag = dag
    )

etl = PythonOperator(
        task_id = "etl",
        python_callable = etl, # 실행할 파이썬 함수
        dag = dag
)

# 저장되었다는 사실 echo
store_result = BashOperator(
    task_id="store_result",
    bash_command= "echo 'ETL Done'",
    dag=dag
)

 # 파이프라인 구성하기
creating_table >> etl >> store_result
