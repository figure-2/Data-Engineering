from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# 각 필드에 컬럼명을 부여하기 위한 클래스 설정
from collections import namedtuple
import os

columns = ["Ticker","Date","Open","High","Low","Close","AdjClose","Volume"]
Finance = namedtuple("Finance", columns)

if __name__ == "__main__":
    sc = (
        SparkSession.builder.master("local[2]")
            .appName("DStream Transformations")
            .getOrCreate().sparkContext
    )
    
    ssc = StreamingContext(sc, 5)

    # 소켓으로부터 데이터를 스트리밍 하되, 컬ㄹ럼 mapping까지 수행
    def read_finance():
        
        # ","를 기준으로 쪼갠 한 줄의 각 필드마다 컬럼 이름을 부여
        def parse(line):
            arr = line.split(",")
            return Finance(*arr)
        
        return ssc.socketTextStream("localhost", 12345).map(parse)

    finance_stream = read_finance()
    # finance_stream.pprint()

    def filter_nvda():
        finance_stream.filter(lambda f : f.Ticker == "NVDA").pprint()
        
    # filter_nvda()

    def filter_volume():
        # vloume이 500만이상인 종목에 대해서만 필터링
        finance_stream.filter(lambda f : int(f.Volume) >= 5000000).pprint()
        
    
    # filter_volume()

    # Ticker별 카운트
    def count_ticker():
        # 스트리밍 환경에서 집계를 하면 전체 데이터에 대한 집계가 아닌, 마이크로 배치 단위 별로 집계가 수행
        finance_stream.map(lambda f : (f.Ticker, 1)).reduceByKey(lambda x, y : x + y).pprint()
    
    # count_ticker()
       
    # 실습. groupByKey를 이용해서 날짜(Date) 별 볼륨(Volume) 합계 구하기. 합계는 파이썬 내장함수인 sum을 이용
    def group_by_date_volume():
        finance_stream.map(lambda f : (f.Date, int(f.Volume))).groupByKey().mapValues(sum).pprint()
    
    # group_by_date_volume()

    def save_to_json(): # 스키마 형식이 아니라서 깔끔하지는 않음
        # Worker에서만 작동하는 함수를 작성 (SAVE)

        def foreach_func(rdd):
            # rdd가 비어있으면 아무것도 하지 않기
            if rdd.isEmpty():
                print("RDD IS EMPTY")
                return
            
            # 데이터가 RDD에 존재하는 경우에는 DataFrame으로 바꿔서 json으로 저장
            df = rdd.toDF(columns)

            dir_path = "/home/ubuntu/working/spark-examples/spark-streaming/data/stocks/outputs"

            # 경로 내에 몇 개의 파일이 있는지 구하고, 인덱스 생성
            n_files = len(os.listdir(dir_path))
            save_file_path = f"{dir_path}/finance-{n_files}.json"

            # json 형식으로 저장
            df.write.json(save_file_path)
            print("Write Complete")

        # 특정 Task를 Worker에서 작동하게 하기 - foreachRDD
        finance_stream.foreachRDD(foreach_func)
        
    save_to_json()
    
    ssc.start()
    ssc.awaitTermination()