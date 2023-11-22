from kafka import KafkaConsumer

BROKER_SERVERS = ['172.31.41.123:9092']
TOPIC_NAME = 'sample-topic'

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = BROKER_SERVERS)

print('wait.....')

# consumer는 topic에 쌓여가는 메시지를 무제한으로 가져와야 한다. - 무한대기
# consumer는 python의 generator로 구현되어 있다.
for message in consumer:
  print(message)