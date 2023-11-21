from datetime import datetime  # 스케쥴링을 위해 시간 정보가 필요
from airflow import DAG

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor

# 데이터를 요청하고 받아오는 operator
from airflow.providers.http.operators.http import SimpleHttpOperator

# Python을 실행하기 위한 operator, python 에서 만듬 함수나 클래스를 airflow에서 실행되게 해줌
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pandas as pd

default_args = {"start_date": datetime(2023, 11, 1)}

# REST API
KAKAO_API_KEY = "REST API"


# 함수 정의
def preprocessing(ti):
    # ti : task instance => DAG 내의 TASK 정보를 얻어 낼 수 있는 객체 (내가 원하는 부분의 정보를 가져올 수 있음)
    search_result = ti.xcom_pull(task_ids=["extract_kakao"])

    print("=========================PREPROCESSING XCOM===============================")
    print(search_result[0]["documents"])

    documents = search_result[0]["documents"]
    df = pd.DataFrame(documents)

    df.to_csv(
        "/home/ubuntu/airflow/dags/processed_result.csv", index=None, header=False
    )


# airflow에서 사용할 DAG를 등록해주는 과정
with DAG(
    dag_id="kakao-pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    tags=["kakao", "api", "pipeline"],
    catchup=False
    # 11월 21일 기준으로 하면 11월 1일부터 11월 20일까지 20일의 공백이 있는데 20일 공백동안의 데이터는 없는것으로 친다
    # False 가 아니라 True면 반대
) as dag:
    # 첫번째 operator : 테이블 생성 ( 일반적인 커리와 틀림)

    creating_table = SqliteOperator(
        task_id="creating_table",  # dag를 관리하는 작은 단위? task_id 2.3
        sqlite_conn_id="sqlite_db",
        sql="""
      CREATE TABLE IF NOT EXISTS kakao_search_result(
        created_at TEXT,
        contents TEXT,
        title TEXT,
        URL TEXT        
      )
    """
        # "IF NOT EXISTS"의 의미 : 테이블이 있으면 생성하지 않는다는
    )

    # HTTPSensor를 이용하여 해당 api에 접속이 가능한지 확인
    # airflow에서 admin connection 할때 host에는 주소가 들어간다 항상
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="kakao_api",
        endpoint="v2/search/web",
        headers={"Authorization": f"KakaoAK {KAKAO_API_KEY}"},
        request_params={"query": "LG 트원스"},
        response_check=lambda response: response.json(),
    )
    # response_check는 홤수로 만들며 함수는 '반드시' 리턴을 해야 한다.
    # 리턴을 하지 않으면 무한 대기하게 된다 (응답이 될 때까지 무한으로 대기)

    # 실제 http 요청 후 데이터 받아오기
    extract_kakao = SimpleHttpOperator(
        task_id="extract_kakao",
        http_conn_id="kakao_api",
        endpoint="v2/search/web",
        headers={"Authorization": f"KakaoAK {KAKAO_API_KEY}"},
        data={"query": "LG 트원스"},
        method="GET",
        response_filter=lambda res: res.json(),
        log_response=True,
    )

    #
    preprocess_result = PythonOperator(
        task_id="preprocess_result", python_callable=preprocessing
    )

    # BashOperator : 리눅스 시스템에서 사용하는 명령어를 실행하기 휘함
    # 데이터를 다루기 위해 사용한는것은 아님. // 리눅스 커멘트를 하기 위한 것임
    store_result = BashOperator(
        task_id="store_result",
        bash_command='echo -e ".separator ","\n.import /home/ubuntu/airflow/dags/processed_result.csv kakao_search_result" | sqlite3 /home/ubuntu/airflow/airflow.db',
    )

    # Operator를 엮어서 파이프라인화 시키기 // '>>' 비트 연산자 사용
    (
        creating_table
        >> is_api_available
        >> extract_kakao
        >> preprocess_result
        >> store_result
    )
