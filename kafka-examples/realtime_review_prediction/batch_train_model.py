import pandas as pd
import numpy as np
import urllib.request
import re

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression

from joblib import dump

# 데이터 다운로드 및 데이터 프레임 생성
def download_review_data():
    urllib.request.urlretrieve("https://raw.githubusercontent.com/e9t/nsmc/master/ratings_train.txt", filename="ratings_train.txt")
    df_train = pd.read_table('ratings_train.txt')

    return df_train

def preprocessing_review_data(df):
    # 1. 한글을 제외한 나머지 데이터 제거 - 필요없는 숫자, 영어 등을 제거
    df = df.dropna(how='any')
    df['document'] = df['document'].apply(lambda s : re.sub("[^ㄱ-ㅎㅏ-ㅣ가-힣 ]", "", s))

    # 2. 한글 빼고 다 제거 했기 때문에 영어만 있거나 특수기호로만 남겨졌던 리뷰는 아무것도 없는 빈 문자열
    df['document'].replace("", np.nan, inplace=True)
    df = df.dropna(how='any')

    return df

if __name__ == "__main__":
    df = download_review_data()
    df = preprocessing_review_data(df)
    
    # 전체 데이터에서 10000개만 랜덤으로 추출
    df_train_toy = df.sample(10000, random_state=42)

    # 머신러닝 모델에 텍스트를 훈련시키기 위해 벡터화 해주는 클래스
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_vectorizer.fit(df_train_toy['document'])

    X_train_tfidf_vector = tfidf_vectorizer.transform(df_train_toy['document'])
    y_train = df_train_toy['label']

    # 모델 훈련
    lr_clf = LogisticRegression()
    lr_clf.fit(X_train_tfidf_vector, y_train)

    print(f"Accuracy : {lr_clf.score(X_train_tfidf_vector, y_train)}")

    # 만들어진 모델을 저장
    dump(lr_clf, "/home/ubuntu/working/kafka-examples/realtime_review_prediction/korean_review_model.pkl")
    dump(tfidf_vectorizer, '/home/ubuntu/working/kafka-examples/realtime_review_prediction/tfidf_vectorizer.pkl')