from datetime import datetime
from airflow import DAG

# naver_examples.py에 접근을 하기 위해 
from crawler.naver_examples import get_naver_finance

# python Operator - 파이썬 코드를 실행하기 위한 오퍼레이터
from airflow.operators.python import PythonOperator

# 판다스 데이터 프레임으로 만든다
import pandas as pd

default_args = {
  'start_date' : datetime(2023, 11, 21)
}

# 리턴된 데이터를 ti.xcom_pull을 이용해서 가져온다

def preprocessing_result(ti):
  naver_finance_data = ti.xcom_pull(task_ids = ['naver_finance_result'])
  
  # 예외 처리 // # 가져온 결과가 없다면 
  if not len(naver_finance_data): 
    raise ValueError('검색 결과가 없습니다.')
  
  result_df = pd.DataFrame(naver_finance_data[0])
  
  # 크롤링하는 시간 지정
  now_time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
  result_df.to_csv(f'/home/ubuntu/airflow/dags/naver_result_{now_time}.csv', index = None, header = True)
  # 인덱스가 있었으면 함녀 header = True// 아니면  header = False
  
  

with DAG (
  dag_id = 'naver-finance-pipeline',
  schedule_interval = '@hourly',
  default_args = default_args,
  tags = ['naver','soup'],
  catchup = False
  ) as dag:
  
  #pass

  # 1. 네이버 증권 정보에서 환율 크롤링
  naver_finance_result = PythonOperator(
    task_id = 'naver_finance_result',
    python_callable = get_naver_finance
  )
  
  # 2. 저장
  naver_finance_save = PythonOperator(
    task_id = 'naver_finance_save',
    python_callable = preprocessing_result
  )
  
  naver_finance_result >> naver_finance_save