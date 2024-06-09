import scrapy
from scrapy_redis.spiders import RedisSpider
import pandas as pd
import re
import redis
from datetime import datetime, timedelta
import time

redisClient = redis.from_url('redis://127.0.0.1:6379')
class M2RedisSpider(RedisSpider):
    name = "m2_redis"
    redis_key = 'data_queue:mark2'
    uniqueemail = set()
    url = ""
    
    # Number of url to fetch from redis on each attempt
    redis_batch_size = 1
    # Max idle time(in seconds) before the spider stops checking redis and shuts down
    max_idle_time = 7200

    def make_request_from_data(self, data):
        #convert data string using eval  to dictionary
        self.url = data.decode('utf-8')
        
        

        return scrapy.Request(url=self.url, dont_filter=True)
    


    def parse(self, response):
        #print(response.text)
        namespaces = {
            'mets': 'http://www.loc.gov/METS/',
            'xlink': 'http://www.w3.org/1999/xlink',
        }
        links = response.xpath('//mets:file/mets:FLocat/@xlink:href', namespaces=namespaces).getall()
        for link in links:
            if link.endswith('.xml'):
                redisClient.lpush('data_queue:mark3', link)
                pdf_url = link.replace('.alto.xml', '.pdf')
                with open('pdf_url_links.txt', 'a') as f:
                    f.write(pdf_url + '\n')
                with open('xml_url_links_SNP.txt', 'a') as f:
                    f.write(link + '\n')
                
