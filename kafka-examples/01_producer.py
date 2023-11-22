from kafka import KafkaProducer

# 브로커 목록을 저으이
# 브로커가 한 개만 있어도 리스트 형식으로 조회
# 여러 개의 브로커가 띄어진 상태에서도 한 개의 브로커만 입력하는 것이 아닌, 모든 브로커의 목록을 입력

BROKER_SERVERS = ['172.31.41.123:9092']
TOPIC_NAME = 'sample-topic'

# producer 객체 생성
producer = KafkaProducer(bootstrap_servers = BROKER_SERVERS)

# 메세지를 토픽에 전송
producer.send(TOPIC_NAME, b'Hello kafka Python') 
# kafka의 파티션에 데이터가 바이너리화 된 상태로 저장
# 앞에 b를 붙이면 바이너리화 된 문자열을(바이트 형식으로??) 보내준다

# 버퍼 플러싱
producer.flush()