from kafka import KafkaConsumer

import json

BROKER_SERVERS = ['172.31.41.123:9092']
TOPIC_NAME = 'json-example'

# 사람이 알아 볼수 있도록 디코딩을 해준다.
consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = BROKER_SERVERS)

for message in consumer:
  json_msg = message.value.decode() # decode()를 수행하면 문자열이 된다
  json_data = json.loads(json_msg)  # 문자열을 json 객체 형태(dict)로 만들어 준다.
  
  print(json_data)