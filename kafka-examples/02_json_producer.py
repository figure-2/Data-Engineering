from kafka import KafkaProducer

import json

BROKER_SERVERS = ['172.31.41.123:9092']
TOPIC_NAME = 'json-example'

#producer = KafkaConsumer(bootstrap_servers = BROKER_SERVERS)
producer = KafkaProducer(bootstrap_servers = BROKER_SERVERS)

sample_json = {
  'name' : '비밀',
  'age' : 23
}

# 반듯이 json.dumps를 이용해서 프로듀싱한다
# utf-8 방식으로 데이터를 바이터리해서 보내준다
producer.send(TOPIC_NAME, json.dumps(sample_json).encode('utf-8'))

producer.flush()


# 반드시 json.dumps를 이용해 프로듀싱한다.
producer.send(TOPIC_NAME, json.dumps(sample_json).encode("utf-8"))

producer.flush()