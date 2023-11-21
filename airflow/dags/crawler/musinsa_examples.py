import requests
import pandas as pd

from bs4 import BeautifulSoup

def get_musinsa():

  URL = "https://www.musinsa.com/category/001001"

  response = requests.get(URL)
  page = response.content
  soup = BeautifulSoup(page, 'html.parser')

  #-------

  product_list = soup.select("#searchList > li")
  sample_item1 = product_list[0]

  #------------
  #product_img_url_list = []
  product_title = []
  product_name = []
  product_price = []
  star_count = []

  data_list = []

  for product in product_list:
    img_url = product.select_one("img.lazyload")['data-original']

    # 브랜드
    if len(product.select("p.item_title")) > 1:
      product_title = product.select("p.item_title")[-1].text
    else:
      product_title = product.select_one("p.item_title").text

    # 상품명
    product_name = product.select_one("p.list_info").text.strip()

    # 가격
    price_del_elem = product.select_one("p.price").find("del")

    # <del> 태그가 있으면..
    if price_del_elem:
      product_price = price_del_elem.nextSibling.strip()
    else: # <del> 태그가 없으면..
      product_price = product.select_one("p.price").text.strip()

    product_price = int(product_price.replace(",", "").replace("원", ""))

    # 별점 등록 수
    #star_count = int(product.select_one("p.point > span.count").text.replace(",", ""))
    star_count = product.select_one("p.point > span.count")
                      
    if star_count is None:
      star_count = 0
    else:
      star_count = int(star_count.text.replace(",", ""))
      
    
    # print(product_title, product_name, product_price, star_count)
    
    
    # product_img_url_list.append(img_url)
    # product_brand_list.append(product_title)
    # product_name_list.append(product_name)
    # product_price_list.append(product_price)
    # product_star_count_list.append(star_count)
    

    data = {
        #"상품이미지URL": product_img_url_list,
        "상품브랜드": product_title,
        "상품명": product_name,
        "상품가격": product_price,
        "상품별점등록수": star_count
    }

    data_list.append(data)
    
    
    # # -------
  return data_list