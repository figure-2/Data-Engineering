from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# dstream 은 rdd를 구현하는 것과 비슷함

if __name__ == '__main__':
    # 1. SparkContext (정의 RDD이기 때문에 SparkContext로 정의해야한다.)
    sc = (
        SparkSession.builder.master('local[2]')
            .appName('Dstream Example')
            .getOrCreate()
            .sparkContext
    )

    # 2. 일반 SparkContext 를 Streaming 이 가능한 Spark Streaming Context로 변환
    ssc = StreamingContext(sc, 5) # 5초마다 새로운 마이크로배치 생성

    # 소켓에서 스트리밍 데이터 불러오기 
    def read_from_socket():
        socket_stream = ssc.socketTextStream('localhost', 12345)

        # Transformations 정의. 불러온 텍스트를 공백 단위로 쪼개기
        word_stream = socket_stream.flatMap(lambda line : line.split())

        # action(콘솔에 print)
        word_stream.pprint()

        ssc.start()
        ssc.awaitTermination()

    # read_from_socket()

    # 파일에서 데이터를 읽어서 스트리밍 
    # flume fluentd 대체 가능

    def read_from_file():
        stocks_file_path = '/home/ubuntu/working/spark-examples/spark-streaming/data/stocks'

        # 스트릶을 파일에서 부터 읽어올 수 있도록 설정. 경로 상에 새롭게 추가된 파일만 읽음! 
        text_stream = ssc.textFileStream(stocks_file_path)

        text_stream.pprint()

        ssc.start()
        ssc.awaitTermination()
    
    read_from_file()



