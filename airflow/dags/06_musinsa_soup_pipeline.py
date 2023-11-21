from datetime import datetime
from airflow import DAG

from crawler.musinsa_examples import get_musinsa

from airflow.operators.python import PythonOperator

import pandas as pd

default_args = {
  'start_date' : datetime(2023, 11, 21)
}

def preprocessing_result(ti):
  musinsa_data = ti.xcom_pull(task_ids = ['musinsa_result'])
  
  if not len(musinsa_data):
    raise ValueError('검색 결과가 없습니다.')
  
  result_df = pd.DataFrame(musinsa_data[0])
  
  now_time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
  result_df.to_csv(f'/home/ubuntu/airflow/dags/musinsa_result_{now_time}.csv', index = None, header = True)


with DAG (
  dag_id = 'musinsa-pipeline',
  schedule_interval = '@daily',
  default_args = default_args,
  tags = ['musinsa','soup'],
  catchup = False
  ) as dag:

  # 1. 무신사 제품 정보 크롤링
  musinsa_result = PythonOperator(
    task_id = 'musinsa_result',
    python_callable = get_musinsa
  )

  # 2. 저장
  musinsa_save = PythonOperator(
    task_id = 'musinsa_save',
    python_callable = preprocessing_result
  )
  
  musinsa_result >> musinsa_save