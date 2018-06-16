import json
import requests
from lxml import html
from collections import OrderedDict
import argparse
import pandas as pd
import datetime as dt
import itertools
from time import sleep
import random
import urllib3
from lxml.html import fromstring
import re
from queue import *
#import boto3
from multiprocessing import Pool
import multiprocessing as mp
import signal
from timeout import timeout
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers_list = [
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_8; en-US) AppleWebKit/532.8 (KHTML, like Gecko) Chrome/4.0.302.2 Safari/532.8'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.464.0 Safari/534.3'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_5; en-US) AppleWebKit/534.13 (KHTML, like Gecko) Chrome/9.0.597.15 Safari/534.13'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.186 Safari/535.1'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.54 Safari/535.2'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.4 (KHTML like Gecko) Chrome/22.0.1229.79 Safari/537.4'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Mac OS X Mach-O; en-US; rv:2.0a) Gecko/20040614 Firefox/3.0.0 '},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10.5; en-US; rv:1.9.0.3) Gecko/2008092414 Firefox/3.0.3'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.1) Gecko/20090624 Firefox/3.5'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.2.14) Gecko/20110218 AlexaToolbar/alxf-2.0 Firefox/3.6.14'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10.5; en-US; rv:1.9.2.15) Gecko/20110303 Firefox/3.6.15'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:5.0) Gecko/20100101 Firefox/5.0'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:9.0) Gecko/20100101 Firefox/9.0'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2; rv:10.0.1) Gecko/20100101 Firefox/10.0.1'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:16.0) Gecko/20120813 Firefox/16.0'},
    {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.15; Mac_PowerPC)'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en-US) AppleWebKit/125.4 (KHTML, like Gecko, Safari) OmniWeb/v563.15'},
    {'User-Agent': 'Opera/9.0 (Macintosh; PPC Mac OS X; U; en)'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML, like Gecko) Safari/85.8'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML, like Gecko) Safari/125.8'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; fr-fr) AppleWebKit/312.5 (KHTML, like Gecko) Safari/312.3'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/418.8 (KHTML, like Gecko) Safari/419.3'}]

def get_proxies(num_proxies = 5):
    print("INTIALIZING {} PROXIES".format(num_proxies))
    # Go to the proxy list url, and get their website text
    # url = 'https://free-proxy-list.net/'
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    # For each potential proxy, add it if it works.
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            try:
                sleep(.5)
                # Check the proxy
                urls = ["http://www.neopets.com/", "https://www.theatlantic.com/", "https://www.reddit.com/r/funny/", "http://www.pythonforbeginners.com/"]
                url = random.sample(urls, 1)[0]
                print(url)
                print(proxy)
                with timeout(seconds=2):
                    response = requests.get(url,proxies={"http": proxy, "https": proxy})
                # If it works, add the proxy
                proxies.add(proxy)
                print("="*50)
                print("We have added {} proxies of a total of {} proxies".format(len(proxies),num_proxies))
                print("{}% Complete with initializing proxies".format(round(len(proxies)/num_proxies*100,2)))
                if len(proxies) == num_proxies:
                    return proxies
            except:
                print("Failed Connection")
                continue
    return proxies

def get_urls():
    urls = []

    for i in range(1,2): ###23
        print("{}% done getting the 100 urls".format(round(i/100*100,2)))
        headers = random.sample(headers_list, 1)
        headers = headers[0]

        sleep(random.uniform(5,8))
        #url_1 = "https://www.amazon.com/s/ref=sr_pg_2?rh=n%3A228013%2Ck%3Asentry+safe&page=" + str(i) + "3&sort=s9-rank&keywords=sentry+safe&ie=UTF8&qid=1528923499"
        # url_1 = "https://www.amazon.com/s/ref=?rh=n%3A228013%2Ck%3Asentry%20safe&page="+ str(i) +"&keywords=sentry%20safe"
        url_1 = "https://www.amazon.com/s/ref=?rh=n%3A228013%2Ck%3Aamazonbasics%20safe&page="+ str(i) +"&keywords=amazonbasics%20safe"
        print(type(url_1))
        print("url_1", url_1)
        print(headers)

        for i in range(5):
            # print("new forloop iteration")
            proxy = random.sample(proxies, 1)
            proxy = proxy[0]
            try:
                r_1 = requests.get(url_1, headers=headers, verify=False, proxies={"http": proxy, "https": proxy}, timeout = 5)
                break
            except Exception as e:
                print ("Error is {}".format(e))
                print ("Retrying...")

        print(url_1, r_1) # print the response of webpage
        soup_1 = BeautifulSoup(r_1.text)
        asins = re.findall('data-asin="(.{10})"', r_1.text)
        print('asins:', asins)
        print('asins:', len(asins))
        # soup_review_num = soup_1.find_all('a', attrs={'class':'a-size-small a-link-normal a-text-normal'})
        # soup_review_num = soup_review_num[1::2]
        # print(soup_review_num)

        # print("{}% done getting the 24 urls".format(round(i/23*100,2)))
        soup_andrew = soup_1.find_all('div', attrs={'class':'s-item-container'})
        print(len(soup_andrew))

        for j in range(3,33): ###33
            print('=' *100)
            print('now it is', j)
            if re.findall('out of 5 stars',str(soup_andrew[j-3])):
                #print(soup_andrew[j])
                page_1 = int(re.findall('customerReviews">(.*?)</a></div>',str(soup_andrew[j-3]))[0].replace(',',''))//10 + 1 ###10
            else:
                continue

            for l in range(1, page_1 + 1):
                    url_2 = 'https://www.amazon.com/product-reviews/'+ asins[j] +'/?pageNumber=' + str(l)
                    print(url_2)
                    urls.append(url_2)

            ASINlist = list(dfcsv['ASIN'])
            for i in range(ASINlist):
                url = 'https://www.amazon.com/product-reviews/' + ASINlist[i] + '/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=1'
                 print(url)
            # url_3 = 'https://www.amazon.com/product-reviews/' + asins[j] + '?pageNumber=1'
            # print(url_3)
            # r_review= requests.get(url_3, headers=headers, verify=False, proxies={"http": proxy, "https": proxy}, timeout = 5)
            # soup_3 = BeautifulSoup(r_review.text)
            # review_page = soup_3.find_all('li', attrs={'data-reftag':'cm_cr_arp_d_paging_btm'})
            # if review_page == []:
            #     continue
            # else:
            #     page_1 = int(review_page[-1].text)
            #     for l in range(1, page_1 + 1):
            #         url_2 = 'https://www.amazon.com/product-reviews/'+ asins[j] +'/?pageNumber=' + str(l)
            #         urls.append(url_2)

    return urls

def parse(url):
    # proxies is global variable
    print("starting parser"+"="*50)
    sleep(random.uniform(5,8))

    # global_count += 1

    # if global_count%5000

    for i in range(5):
        print("new for loop iteration")
        proxy = random.sample(proxies, 1)
        prev_proxy = proxy

        try:
            # # Get a random proxy, make sure it was not the most recent proxy
            while proxy == prev_proxy:
                proxy = random.sample(proxies, 1)
                proxy = proxy[0]
                print("Getting different proxy")
            prev_proxy = proxy

            # Get random header
            headers = random.sample(headers_list, 1)
            headers = headers[0]

            print("header", headers)
            print("proxy", proxy)

            print("got headers, getting url and starting response")

            r_2 = requests.get(url, headers=headers, verify=False, proxies={"http": proxy, "https": proxy}, timeout = 5)
            if r_2.status_code == 200:
                
                 # Actual information
                
                len_1 = []
                # Initialize list of reviews
                review_list = []
                soup_2 = BeautifulSoup(r_2.text)
                soup_review = soup_2.find_all('span', attrs={'data-hook':'review-body'})
                len_review = len(soup_review)
                soup_date = soup_2.find_all('span', attrs={'data-hook':'review-date'})
                len_date = len(soup_date)
                soup_rating = soup_2.find_all('i', attrs={'data-hook':'review-star-rating'})
                len_rating = len(soup_rating)
                soup_title = soup_2.find_all('a', attrs={'data-hook':'review-title'})
                len_title = len(soup_title)
                len_1 = [len_review, len_date, len_rating, len_title]
                if len(set(len_1)) != 1:
                    print('jumping out of loop')
                    return{"error":"failed to process the page",}
                else:
                    # For each page, get all reviews and append them in a list of dictionaries
                    for m in range(len_review):
                        soup_price = soup_2.find_all('span', attrs={'class':'a-color-price arp-price'})[0].text.replace(',','')
                        price = soup_price
                        soup_name = soup_2.find_all('h1', attrs={'class':'a-size-large a-text-ellipsis'})
                        name = soup_name[0].text
                        title = soup_title[m].text
                        rating = soup_rating[m].text
                        date = soup_date[m].text
                        review = soup_review[m].text
                        product_review={
                        'price':price,
                        'name':name,
                        'title':title,
                        'rating':rating,
                        'date':date,
                        'review':review}

                        # Adding dictionary to list
                        review_list.append(product_review)

                # Add dictionary here for response object
                return review_list
            else: 
                print('Error Code {}'.format(response.status_code))

        except Exception as e:
            print ("Error is {}".format(e))
            print ("Retrying...")
            
        return {"error":"failed to process the page",}

# Generating global proxy list
num_proxies = 25
proxies = get_proxies(num_proxies)

if __name__=="__main__":

    # Getting all the urls of the Amazon products.
    # After, will get all reviews at each product page.
    urls = get_urls()
    
    # dfcsv = pd.read_csv('/Users/Tony/Desktop/MasterLock/ASINs.csv')


    print("Starting multiprocessing")
    pool = mp.Pool(10)
    results = pool.map(parse, urls)
    pool.terminate()
    pool.join()
    print("done with results")

    with open('amazon_results.csv','w') as fp:
        csv.dump(results,fp)















# lists_1 = []
# # lists_2 = []
# for i in range(1,24):
#     time.sleep(2)
#     url_1 = "https://www.amazon.com/s/ref=?rh=n%3A228013%2Ck%3Asentry%20safe&page="+ str(i) +"&keywords=sentry%20safe"
#     #headers =
#     r_1 = requests.post(url_1)
#     print(url_1, r_1) # print the response of webpage
#     soup_1 = BeautifulSoup(r_1.text)
#     #soup_price = soup_1.find_all('span', attrs={'class':'a-size-base a-color-base'})
#     asins = re.findall('data-asin="(.{10})"', r_1.text)
#     print(len(asins))
#     soup_review_num = soup_1.find_all('a', attrs={'class':'a-size-small a-link-normal a-text-normal'})
#     soup_review_num = soup_review_num[1::2]
#     for j in range(len(asins)):
#         page_1 = int(soup_review_num[j].text.replace(',',''))//10 + 1
#         for l in range(page_1 + 1):
#             len_1 = []
#             url_2 = 'https://www.amazon.com/product-reviews/'+ asins[j] +'/?pageNumber=' + str(l)
#             #headers = and proxies
#             r_2 = requests.post(url_2)



#             # Actual information
#             print(url_2, r_2)
#             result_list = []
#             soup_2 = BeautifulSoup(r_2.text)
#             soup_review = soup_2.find_all('span', attrs={'data-hook':'review-body'})
#             len_review = len(soup_review)
#             soup_date = soup_2.find_all('span', attrs={'data-hook':'review-date'})
#             len_date = len(soup_date)
#             soup_rating = soup_2.find_all('i', attrs={'data-hook':'review-star-rating'})
#             len_rating = len(soup_rating)
#             soup_title = soup_2.find_all('a', attrs={'data-hook':'review-title'})
#             len_title = len(soup_title)
#             len_1 = [len_review, len_date, len_rating, len_title]
#             if len(set(len_1)) != 1:
#                 print('jumping out of loop')
#                 return{"error":"failed to process the page",}
#             else:
#                 for m in range(len_review):
#                     soup_price = soup_2.find_all('span', attrs={'class':'a-color-price arp-price'})[0].text.replace(',','')
#                     price = soup_price
#                     soup_name = soup_2.find_all('h1', attrs={'class':'a-size-large a-text-ellipsis'})
#                     name = soup_name[0].text
#                     title = soup_title[m].text
#                     rating = soup_rating[m].text
#                     date = soup_date[m].text
#                     review = soup_review[m].text
#                     product_review={
#                     'price':price,
#                     'name':name,
#                     'title':title,
#                     'rating':rating,
#                     'date':date,
#                     'review':review}

#                     # Adding dictionary to list
#                     result_list.append(product_review)


