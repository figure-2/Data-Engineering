from kafka import KafkaConsumer

import json
from joblib import load

TOPIC_NAME = "naver_reviews"
BROKER_SERVERS = ['172.31.41.123:9092']

if __name__ == '__main__':
  consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = BROKER_SERVERS)
  
  TFIDF_VECTORIZER_PATH = '/home/ubuntu/working/kafka-examples/realtime_review_prediction/tfidf_vectorizer.pkl'
  MODEL_PATH = '/home/ubuntu/working/kafka-examples/realtime_review_prediction/korean_review_model.pkl'
  
  # pkl 파일로 구성된 Vectorizer와 머신러닝 모델 불러오기
  tfidf_vectorizer = load(TFIDF_VECTORIZER_PATH)
  model = load(MODEL_PATH)
  
  for msg in consumer:
    sentence = json.loads(msg.value.decode())['sentence']
    
    # 벡터화
    test_vector = tfidf_vectorizer.transform(sentence)
    
    # 예측
    prediction - model.predict(test_vector)[0]
    
    print(sentence, '====>', prediction)