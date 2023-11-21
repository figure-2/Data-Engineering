# Spakr 작업이 들어갈 어플리케이션
from pyspark.sql import SparkSession

# python에서 메인 함수 역황을 한다.
MAX_MEMEORY = '5g'
spark = (
        SparkSession.builder.appName("taxi-fare-prediction")
            .config("spark.executor.memory", MAX_MEMEORY)
            .config("spark.driver.memory", MAX_MEMEORY)
            .getOrCreate())

directory = '/home/ubuntu/working/datasource'
trip_files = '/trips/*'

trip_df = spark.read.csv(f"file:///{directory}/{trip_files}", inferSchema = True, header = True)
#trip_df.printSchema()
trip_df.createOrReplaceTempView('trips')

# 데이터 정제
# 쥬피터 노트북에서 했던 작업을 한번에 구현
# vscode에서는 sql 실행 결과를 보기 어려우니깐 주피터 노트북에서 확인하고 vscode에서 진행
query = """
SELECT
    t.passenger_count,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    t.trip_distance,
    HOUR(tpep_pickup_datetime) as pickup_time,
    DATE_FORMAT(TO_DATE(tpep_pickup_datetime), 'EEEE') as day_of_week,
    
    t.total_amount

FROM trips t

WHERE t.total_amount < 200
  AND t.total_amount > 0
  AND t.passenger_count < 5
  AND TO_DATE(t.tpep_pickup_datetime) >= '2021-01-01'
  AND TO_DATE(t.tpep_pickup_datetime) < '2021-08-01'
  AND t.trip_distance < 10
  AND t.trip_distance > 0
"""
# 정제 쿼리 실행 (트렌스포메이션 작업)
data_df = spark.sql(query)

# 훈련 / 테스트 세트 분리
train_sdf, test_sdf = data_df.randomSplit([0.8, 0.2], seed = 42)

# 2 : 5
# 훈련을 하는것이 아니 때문에 
# 전처리하고 하이퍼파라미터 튜닝하고 새로운 모델 훈련을 하기 때문에
# sql을 활용해서 전처리를 함 그렇기 때문에 여기서는 전처리를 안해도 된다.
# 프로그램을 구현할때 세분화해서 내가 목적을 가지고 있는 부분만 구현??
# oop 개념이 필요하다 (모듈을 만든다) -> CBD(하나의 독립적으로 돌아갈 수 있는 컴포넌트)
# 지금은 전처리 컴포넌트를 만든다. 그래서 이제 훈련/ 테스트를 저장한다

# 훈련 / 테스트 데이터 저장
data_dir = '/home/ubuntu/airflow/ml-data'

# parquet 형식으로 저장 요즘 추천하는 저장 방식임
train_sdf.write.format('parquet').mode('overwrite').save(f"{data_dir}/train/")
test_sdf.write.format('parquet').mode('overwrite').save(f"{data_dir}/test/")

# 위에 까지 해서 전처리를 하기 위한 컴포넌트를 완성.

spark.stop()



##################################################################################################

# # Spark 작업이 들어갈 어플리케이션  airflow랑 전혀 연관 없음. 
# from pyspark.sql import SparkSession

# MAX_MEMEORY = '5g'
# spark = (
#         SparkSession.builder.appName("taxi-fare-prediction")
#             .config("spark.executor.memory", MAX_MEMEORY)
#             .config("spark.driver.memory", MAX_MEMEORY)
#             .getOrCreate())

# directory = '/home/ubuntu/working/datasource'
# trip_files = '/trips/*'

# trips_df = spark.read.csv(f'file:///{directory}/{trip_files}', inferSchema=True, header=True)
# trips_df.printSchema()