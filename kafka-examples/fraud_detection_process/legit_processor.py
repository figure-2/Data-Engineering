# 정상 데이터를 처리하기 위한 컨슈머
from kafka import KafkaConsumer
import json

LEGIT_TOPIC = 'legit_payments'
BROKER_SERVERS = ['172.31.41.123:9092']

if __name__ == "__main__":
    consumer = KafkaConsumer(LEGIT_TOPIC, bootstrap_servers=BROKER_SERVERS)
    
    for message in consumer:
        msg = json.loads(message.value.decode())
        
        payment_type = msg['payment_type']
        payment_date = msg['date']
        payment_time = msg['time']
        amount = msg['amount']
        to = msg['to']
        
        print(f"[정상 결제 정보] : {payment_type} {payment_date} {payment_time} {amount}원 ({to})")