from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == '__main__':
  spark = (
    SparkSession.builder.master('local[2]')
      .appName('Streaming DataFrame Join')
      .getOrCreate()
  )
  
  # 1. 기본 배치 JOIN
  
  authors = spark.read.option('inferSchema', True).json('/home/ubuntu/working/spark-examples/spark-streaming/data/authors.json')
  books = spark.read.option('inferSchema', True).json('/home/ubuntu/working/spark-examples/spark-streaming/data/books.json')
  
  # 기준을 authors, join 수행할 데이터는 books
  
  author_books_df = authors.join(books, authors['book_id'] == books['id'], 'inner')
  author_books_df.show()
  
  # 2. 1개의 데이터 프레임은 스트리밍되고, 다른 한개의 데이터 프레임 배치
  def stream_join_with_static():
    
    streamd_books = (
      spark.readStream.format('socket')
        .option('host', 'localhost')
        .option('port', 12345)
        .load()
        # 로드 까지 해온것 value 까지만 있음 // 이제 value에서 원하는것을 변환하면서 select 진행
        .select(
          F.from_json(F.col('value'), books.schema).alias('book')
          # json 데이터에서 스키마를 입력하면 내가 원하는 형태로 출력됨 // books 라는 데이터 프레임이 있어서 특수하게 스키마를 쓸수 있다/ 원래는 스키마를 직접 만들어야 함
        )
        .selectExpr(
          'book.id as id',
          'book.name as name',
          'book.year as year'
        )
    )
    
    # inner join 수행 시에는 반드시 왼쪽에 위치한 데이터 프레임이 '배치 데이터 프레임' 이어야 한다.
    author_books_df = authors.join(
      streamd_books, authors['book_id'] == streamd_books['id'], 'left'
    )
    
    streamd_books.writeStream.format("console").outputMode("append").start().awaitTermination()
  
  #stream_join_with_static()
  
  # 3. 양 쪽 모두 스트레밍  되는 경우
  
  
    # value 형식이 라니라 csv
    # csv 파일은 with컬럼을 쓰면서 인덱스 기준으로 짤라서 가져왔음
    # json 파일은 바로 컬럼으로 만들어서 (스키마만 따로 정의하면됨)