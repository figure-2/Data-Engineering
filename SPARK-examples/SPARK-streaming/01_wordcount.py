from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# spark 환경 설절
spark = SparkSession.builder.appName("stream-word-count").getOrCreate()

# 실시간 데이터를 받을 데이터 프레임 정의
# 어디서 (소스) 데이터를 스트리밍 할지

# format : 어디서 스트리밍 할지를 정의
# load : 데이터 프레임 생성
lines_df = spark.readStream.format('socket').option('host', 'localhost').option('port', 9999).load()

# expr : select 절에서 사용할 표현식을 문자열로 작성할 수 있게 해준다.

# 한 줄씩 문자열을 받아오고 나서 단어 단위로 쪼개기
words_df = lines_df.select(
  F.expr("explode(split(value, '')) as word")
)

# 단어 별 개수 세기
counts_df = words_df.groupby('word').count()


# 결과물 출력
counts_df.writeStream.format('console').outputMode('complete').start().awaitTermination()