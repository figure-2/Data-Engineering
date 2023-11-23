from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == '__main__':
  
  spark = (
    SparkSession.builder.master('local[2]')
      .appName('streaming DataFrame Examples')
      .getOrCreate()
  )
  
  def simple_streaming():
    lines = (
      spark.readStream.format('socket')
        .option('host', 'localhost')
        .option('port', 12345)
        .load()
    )
    
    lines.writeStream.format('console').outputMode('append').trigger(
      processingTime='2 seconds' # 2초에 한번씩 마이크로 배치 실행
    ).start().awaitTermination()
    
  #simple_streaming()
  
  def read_from_sockrt():
    lines = (
      spark.readStream.format('socket')
        .option('host', 'localhost')
        .option('port', 12345)
        .load()
    )
    
    # 컬럼 정의
    cols = ["ip", "timestamp", "method", "endpoint", "status_code", "latency"]
    
    df = (
      lines.withColumn('ip', F.split(lines['value'], ',').getItem(0))
        .withColumn('timestamp', F.split(lines['value'], ',').getItem(1))
        .withColumn('method', F.split(lines['value'], ',').getItem(2))
        .withColumn('endpoint', F.split(lines['value'], ',').getItem(3))
        .withColumn('status_code', F.split(lines['value'], ',').getItem(4))
        .withColumn('latency', F.split(lines['value'], ',').getItem(5))
        .select(cols)
    )
    
    # status_code : 400이고, endpoint가 user인 row만 필터링
    #df = df.filter((df.status_code == '400') & (df.endpoint == '/users'))
    
    group_cols = ['method', 'endpoint']
    
    df = df.groupby(group_cols).agg(
      F.max('latency').alias('max_latency'),
      F.min('latency').alias('min_latency'),
      F.mean('latency').alias('mean_latency')
      )
    
    
    df.writeStream.format('console').outputMode('complete').start().awaitTermination()
    #df.writeStream.format('console').outputMode('append').start().awaitTermination()
  
  read_from_sockrt()