from datetime import datetime
from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 
default_args = {
  'start_date' : datetime(2023, 11, 1)
}

with DAG(
  dag_id='spark-ml-pipline',
  schedule_interval='@daily',
  default_args=default_args,
  tags=['spark', 'ml', 'taxi'],
  catchup=False) as dag:
  
  preprocess = SparkSubmitOperator (
    task_id = 'preprocess',
    conn_id = 'spark_local',
    # 제출한 job을 만들기 // 작업이 명시된 파이썬 파일 (프리프로세스 파일을 받아온다)
    application = '/home/ubuntu/airflow/dags/03_preprocess.py' 
  )
  
  tune_hyperparameter = SparkSubmitOperator (
    conn_id = 'spark_local',
    task_id = 'tun_hyperparameter',
    application = '/home/ubuntu/airflow/dags/04_tune_hyperparameter.py'
  )
  
  train_model = SparkSubmitOperator (
    conn_id = 'spark_local',
    task_id = 'train_model',
    application = '/home/ubuntu/airflow/dags/05_train_model.py'
  )
  
  preprocess >> tune_hyperparameter >> train_model