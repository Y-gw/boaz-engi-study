# boaz_etl_with_xcoms.py
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


def extract(**context):
    link = context["params"]["url"]
    f = requests.get(link)
    return f.text

def transform(**context): # extract에서의 데이터를 컨텍스트로 가져온다
		# 태스크에서의 데이터는 context에서 task_instance로 가져올 수 있다
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    soup = BeautifulSoup(text, 'html.parser')
    titles = []
    tmp = soup.select(".title")
    for title in tmp:
        titles.append(title.get_text())
    return titles

def load(**context): # load에서의 데이터를 컨텍스트로 가져온다
    titles = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    with open("job_search_result.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(titles)



# DAG 설정
dag = DAG(
    dag_id="boaz_etl",
    schedule_interval="@daily",
    start_date=datetime(2022,6,15),
    default_args = { 
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
    }, 
    tags=["BOAZ"], 
    catchup=False 
    ) 


# Operator 만들기
creating_table = PostgresOperator(
        task_id="creating_table",
        postgres_conn_id="postgres_conn_id",
        sql='''
            CREATE TABLE IF NOT EXISTS job_search_result ( 
                title TEXT
            )
        ''',
        dag = dag
    )

extract = PythonOperator(
        task_id = "extract",
        python_callable = extract, # 실행할 파이썬 함수
        params = {
            'url': Variable.get("job_search_url")
        },
        dag = dag
)

transform = PythonOperator(
        task_id = "transform",
        python_callable = transform,
        dag = dag
)

# 검색 결과 전처리하고 CSV 저장
load = PythonOperator(
        task_id = "load",
        python_callable = load, 
        dag = dag
)

# 저장되었다는 사실 echo
store_result = BashOperator(
    task_id="store_result",
    bash_command= "echo 'ETL Done'",
    dag=dag
)

 # 파이프라인 구성하기
creating_table >> extract >> transform >> load >> store_result
