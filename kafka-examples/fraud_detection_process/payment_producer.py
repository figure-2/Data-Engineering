from kafka import KafkaProducer

import datetime
import pytz
import time
import random
import json

BROKER_SERVERS = ['172.31.41.123:9092']
TOPIC_NAME = 'payments'

# 리눅스 시간이 아닌, 서울 시간으로 시간 데이터를 생성하기 위한 함수
def get_seoul_datetime():
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    kst_now = utc_now.astimezone(pytz.timezone("Asia/Seoul"))
    
    d = kst_now.strftime("%Y/%m/%d")
    t = kst_now.strftime("%H:%M:%S")
    
    return d, t
  
# 임시 결제 정보 데이터 발생기 - 랜덤하게 결제 정보를 만듦
def generate_payment_data():
    # 데이터 소스로부터 데이터를 끌어오는 기능을 구현해 주시면 됩니다.(크롤링)
    # 크롤링이나 다른 데이터 레이크에서 데이터를 끌어 올 수도 있다.
    
    # 결제 방식
    payment_type = random.choice(["롯데카드", "삼성카드", "현대카드", "비트코인", "이더리움"])
    
    # 결제 금액
    amount = random.randint(1000, 10000000)
    
    # 결제자
    to = random.choice(["me", "mom", "dad", "stranger"])
    
    return payment_type, amount, to
  
if __name__ == '__main__':
  
  producer = KafkaProducer(bootstrap_servers = BROKER_SERVERS)
  
  # 데이터 발생 및 스트리밍
  while True:
    
    # 현재 시간 데이터 얻기
    d, t = get_seoul_datetime()
    
    #랜덤하게 만들어진 결재 정보 얻기
    payment_type, amount, to = generate_payment_data()
    
    # 스트리밍할 데이터 조립
    row_data = {
      'data' : d,
      'time' : t,
      'payment_type' : payment_type,
      'amount' : amount,
      'to' : to
    }
    
    # 바이너리화 해서 데이터를 토픽에 전송
    producer.send(TOPIC_NAME, json.dumps(row_data).encode('utf-8'))
    producer.flush()
    
    print(row_data)
    time.sleep(1)
    
    
    