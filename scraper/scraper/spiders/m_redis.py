import scrapy
from scrapy_redis.spiders import RedisSpider
import pandas as pd
import re
import redis
from datetime import datetime, timedelta
import time

def generate_date_strings(start_date, end_date):
    date_format = "%Y%m%d"
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]

    date_strings = []
    for date in date_generated:
        date_strings.append(date.strftime(date_format))
    return date_strings


class MRedisSpider(RedisSpider):
    name = "m_redis"
    redis_key = 'data_queue:mark3'
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
        # XPath to select all TextBlock elements in the XML
        namespaces = {
        'ns': 'http://www.loc.gov/standards/alto/ns-v2#',
    }

    # Query all TextBlock elements using the defined namespace
        #Page
        attributes = {}
        # Define the URL
        url = response.url
        pdf_url = url.replace('.alto.xml', '.pdf')
        with open('pdf_url.txt', 'a') as f:
            f.write(pdf_url + '\n')
        print(url)

        # Define a regular expression pattern to extract the date and page parts from the URL
        
# Split the URL by the '/' character to isolate the relevant part
        parts = url.split('/')

        # The relevant part is the last part of the URL before the '.alto.xml'
        filename = parts[-1].replace('.alto.xml', '')

        # Now split the file name by the '-' character
        filename_parts = filename.split('-')

        # Assign the parts to variables
        ref = filename_parts[0]
        date = filename_parts[1]
        # Format the date
        year = date[:4]
        month = date[4:6]
        day = date[6:8]
        date = f'{year}-{month}-{day}'
        page_number = '-'.join(filename_parts[2:])
        page = response.xpath('//ns:Page', namespaces=namespaces)
        attributes['ID'] = page.xpath('./@ID', namespaces=namespaces).get()
        attributes['Reference'] = ref
        attributes['Date'] = date
        attributes['Page_Number'] = page_number


        text_blocks = response.xpath('//ns:TextBlock', namespaces=namespaces)
        # Define your header columns
        # columns = ["ID", "Reference", "Date", "Page_Number", "block_ID", "String", "VPOS", "HPOS", "WIDTH", "HEIGHT", "styrefs"]

        # # Create an empty DataFrame with these columns
        # df = pd.DataFrame(columns=columns)
        # # Write the empty DataFrame with the header to the CSV file
        # # Define filename using variables (make sure they are defined before doing this)
        # filename = f"{ref}_{date}_{page_number}.csv"
        # df.to_csv(filename, index=False)

        


        for text_block in text_blocks:
            # Extract attributes of the TextBlock element
            # attributes = {
            #     # 'VPOS': text_block.xpath('./@VPOS', namespaces=namespaces).get(),
            #     # 'HPOS': text_block.xpath('./@HPOS', namespaces=namespaces).get(),
            #     # 'WIDTH': text_block.xpath('./@WIDTH', namespaces=namespaces).get(),
            #     # 'HEIGHT': text_block.xpath('./@HEIGHT', namespaces=namespaces).get(),
            #     'block_ID': text_block.xpath('./@ID', namespaces=namespaces).get()
            # }
            attributes['block_ID'] = int(text_block.xpath('./@ID', namespaces=namespaces).get().replace('block','').strip())
            strings = text_block.xpath('.//ns:String', namespaces=namespaces)
            all_strings = []
            
            for string in strings:
                content = string.xpath('./@CONTENT', namespaces=namespaces).get()
                vpos = string.xpath('./@VPOS', namespaces=namespaces).get()
                hpos = string.xpath('./@HPOS', namespaces=namespaces).get()
                width = string.xpath('./@WIDTH', namespaces=namespaces).get()
                height = string.xpath('./@HEIGHT', namespaces=namespaces).get()
                #STYLEREFS
                styrefs = string.xpath('./@STYLEREFS', namespaces=namespaces).get()
                attributes['String'] = content
                attributes['VPOS'] = vpos
                attributes['HPOS'] = hpos
                attributes['WIDTH'] = width
                attributes['HEIGHT'] = height
                attributes['styrefs'] = styrefs
                yield attributes
                # df = pd.DataFrame([attributes],columns=columns)
                # df.to_csv(f"{ref}_{date}_{page_number}.csv", index=False, mode='a', header=False)
