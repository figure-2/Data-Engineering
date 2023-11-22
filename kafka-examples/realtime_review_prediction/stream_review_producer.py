from kafka import KafkaProducer
import pandas as pd
import urllib.request
import time
import json

TOPIC_NAME = "naver_reviews"
BROKER_SERVERS = ['172.31.41.123:9092']

def download_test_data():
    urllib.request.urlretrieve("https://raw.githubusercontent.com/e9t/nsmc/master/ratings_test.txt", filename="ratings_test.txt")
    df_test = pd.read_table('ratings_test.txt')

    return df_test

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=BROKER_SERVERS)
    df_test = download_test_data()

    for sentence in df_test['document']:
        # 프로듀서에서는 전처리 하지 않는다.
        msg = {
            "sentence": sentence
        }

        producer.send(TOPIC_NAME, json.dumps(msg).encode("utf-8"))
        print(msg)
        time.sleep(1)

        producer.flush()