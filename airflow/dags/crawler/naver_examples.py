import requests
from bs4 import BeautifulSoup


# 하나의 함수지만 어떤 모듈이나, task로써의 기능을 하고 있다
def get_naver_finance():

  URL = "https://finance.naver.com/marketindex/"

  response = requests.get(URL)
  page = response.content # 접속한 웹 사이트의 html 코드를 가져오기
  soup = BeautifulSoup(page, 'html.parser')

  exchange_list = soup.select_one("#exchangeList")
  fin_list = exchange_list.find_all("li") # exchange_list.select("li")

  # c_name_list = []
  # exchange_rate_list = []
  # change_list = []
  # updown_list = []

  data_list = []

  for fin in fin_list:
    c_name = fin.select_one("h3.h_lst").text
    exchange_rate = fin.select_one("span.value").text
    change = fin.select_one("span.change").text
    updown = fin.select_one("span.change").nextSibling.nextSibling.text
    
    print(c_name, exchange_rate, change, updown)
    
    data = {
      'c_name' : c_name,
      'exchange_rate' : exchange_rate,
      'change' : change,
      'updown' : updown
    }
    
    data_list.append(data)
    
  # # print(data_list)
  # # print(c_name)

  #작업의 결과를 반환 >  - XCOM을 활용하기 위해 리
  return data_list


# 다음 task(프로세스)로 data_liat를 넘기기 위해 xcom을 사용
# 그래서 위에 내용 전체를 함수로 만들어야 한다.
