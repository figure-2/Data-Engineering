from datetime import datetime
from airflow import DAG

from crawler.musinsa_examples import get_musinsa

from airflow.operators.python import PythonOperator

import pandas as pd

from pyspark.sql import SparkSession

MAX_MEMEORY = "5g"
spark = (
    SparkSession.builder.appName("taxi-fare-prediction")
    .config("spark.executor.memory", MAX_MEMEORY)
    .config("spark.driver.memory", MAX_MEMEORY)
    .getOrCreate()
)


default_args = {"start_date": datetime(2023, 11, 21)}


def preprocessing_result(ti):
    musinsa_data = ti.xcom_pull(task_ids=["musinsa_result"])

    if not len(musinsa_data):
        raise ValueError("결과가 없습니다")

    result_df = pd.DataFrame(musinsa_data[0])

    now_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    result_df.to_csv(
        f"/home/ubuntu/airflow/dags/musinsa_result_{now_time}.csv",
        index=None,
        header=True,
    )
    result_sdf = spark.createDataFrame(data=result_df)

    return result_sdf


# 집계


def aggregate_result(ti):
    result_sdf = ti.xcom_pull(task_ids=["musinsa_save"])
    result_sdf.createOrReplaceTempView("musinsa")

    query = """
        SELECT 
            brand, COUNT(*) AS count, AVG(price) AS avg_price, AVG(stars) AS avg_stars
        FROM musinsa
        GROUP BY brand
        ORDER BY count DESC
    """
    result_agg = spark.sql(query)

    result_agg.write.option("header", True).csv("/home/ubuntu/airflow/dags")


with DAG(
    dag_id="musinsa-pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    tags=["musinsa", "soup"],
    catchup=False,
) as dag:
    # 1. 무신사 반팔티 정보에서 크롤링
    musinsa_result = PythonOperator(
        task_id="musinsa_result", python_callable=get_musinsa
    )
    # 2. 저장
    musinsa_save = PythonOperator(
        task_id="musinsa_save", python_callable=preprocessing_result
    )

    # 3. 브랜드별 집계
    musinsa_agg = PythonOperator(
        task_id="musinsa_agg", python_callable=aggregate_result
    )

    musinsa_result >> musinsa_save >> musinsa_agg

    spark.stop()
