# payments 토픽에서 데이터를 consume

# 정상 데이터 => legit_processor로 produce
# 이상 데이터 => fraud_processor로 produce

from kafka import KafkaConsumer, KafkaProducer
import json

PAYMENT_TOPIC = 'payments'

FRAUD_TOPIC = 'frud_payments'
LEGIT_TOPIC = 'legit_patments'

BROKER_SERVERS = ['172.31.41.123:9092']

# 이상 결제 기준 정의
def is_suspicious(message): # message에 JSON 데이터가 들어옵니다.
    # stranger가 결제 or 결제 금액이 500만원 이상이면 이상 결제로 결정.
    #  메시지를 feature로 받는 머신러닝 모델의 predict
    if message["to"] == 'stranger' or message['amount'] >= 5000000:
        return True
    else:
        return False
      
if __name__ == '__main__':
  
  # payments에서 데이터를 받아 올 consumer 정의
  consumer = KafkaConsumer(PAYMENT_TOPIC, bootstrap_servers = BROKER_SERVERS)
  
  # LEGIT_TOPIC 또는 FRAUD_TOPIC에 데이터를 전송할 producer 정의
  producer = KafkaProducer(bootstrap_severs = BROKER_SERVERS)
  
  for message in consumer:
    
    msg = json.loads(message.value.decode())
        
    if is_suspicious(msg):
        print('이상 결제 정보 발생!', msg)
        producer.send(FRAUD_TOPIC, json.dumps(msg).encode('utf-8'))
    else:
        print('정상 결제 정보 발생!', msg)
        producer.send(FRAUD_TOPIC, json.dumps(msg).encode('utf-8'))
        
    producer.flush()