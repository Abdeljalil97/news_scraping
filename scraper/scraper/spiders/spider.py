import scrapy
import re
import redis
from datetime import datetime, timedelta
import time
import pandas as pd
# Function to generate date strings for the URLs


redisClient = redis.from_url('redis://127.0.0.1:6379')

def generate_date_strings(start_date, end_date):
    date_format = "%Y%m%d"
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]

    date_strings = []
    for date in date_generated:
        date_strings.append(date.strftime(date_format))
    return date_strings
class SpiderSpider(scrapy.Spider):
    name = "spider"
    
    def start_requests(self):
        #https://content.staatsbibliothek-berlin.de/zefys/SNP26120215-19450523-0-0-0-0.xml
        base_url = "https://content.staatsbibliothek-berlin.de/zefys/SNP26120215-{}-0-0-0-0.xml"
        start_date = '19460423'
        #end_date = '19910101'
        end_date = '19940101'
        urls = []
        # Generate the URLs
        date_strings = generate_date_strings(start_date, end_date)
        for date_string in date_strings:
            # Construct the URL for the given date
            url = base_url.format(date_string)
            urls.append(url)
        for url in urls:
            redisClient.lpush('data_queue:mark2', url)

            #yield scrapy.Request(url=url, callback=self.parse)    
    def parse(self, response):
        #print(response.text)
        namespaces = {
            'mets': 'http://www.loc.gov/METS/',
            'xlink': 'http://www.w3.org/1999/xlink',
        }
        links = response.xpath('//mets:file/mets:FLocat/@xlink:href', namespaces=namespaces).getall()
        for link in links:
            if link.endswith('.xml'):
                redisClient.lpush('data_queue:mark', link)
                #yield scrapy.Request(url=link, callback=self.parse_xml)
                
    
    def parse_xml(self, response):
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
        columns = ["ID", "Reference", "Date", "Page_Number", "block_ID", "String", "VPOS", "HPOS", "WIDTH", "HEIGHT", "styrefs"]

        # Create an empty DataFrame with these columns
        df = pd.DataFrame(columns=columns)
        # Write the empty DataFrame with the header to the CSV file
        # Define filename using variables (make sure they are defined before doing this)
        filename = f"{ref}_{date}_{page_number}.csv"
        df.to_csv(filename, index=False)

        


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
                df = pd.DataFrame([attributes],columns=columns)
                df.to_csv(f"{ref}_{date}_{page_number}.csv", index=False, mode='a', header=False)
                #all_strings.append(attributes)
                #yield attributes
            # df = pd.DataFrame(all_strings)
            # df.to_csv(f"{ref}_{date}_{page_number}.csv", index=False)
            
                
#                        <String WC="0.87" STYLE="bold" CONTENT="ZENTRALORGAN" VPOS="517.31335" HPOS="190.5" WIDTH="811.9533" HEIGHT="55.033333" STYLEREFS="size20.0"/>

            #     print(content)
            # print(attributes)

            # # Extracts the content of TextLine and String within each TextBlock
            # text_lines_content = []
            # text_lines = text_block.xpath('TextLine')
            # for text_line in text_lines:
            #     strings = text_line.xpath('String')
            #     line_content = {
            #         'VPOS': text_line.xpath('@VPOS').get(),
            #         'HPOS': text_line.xpath('@HPOS').get(),
            #         'WIDTH': text_line.xpath('@WIDTH').get(),
            #         'HEIGHT': text_line.xpath('@HEIGHT').get(),
            #         'Strings': [
            #             {
            #                 'CONTENT': string.xpath('@CONTENT').get(),
            #                 'WC': string.xpath('@WC').get(),
            #                 'STYLE': string.xpath('@STYLE').get(),
            #                 'VPOS': string.xpath('@VPOS').get(),
            #                 'HPOS': string.xpath('@HPOS').get(),
            #                 'WIDTH': string.xpath('@WIDTH').get(),
            #                 'HEIGHT': string.xpath('@HEIGHT').get(),
            #                 'STYLEREFS': string.xpath('@STYLEREFS').get(),
            #             }
            #             for string in strings
            #         ]
            #     }
            #     text_lines_content.append(line_content)

            # # Create a dictionary to store all TextBlock information
            # text_block_data = {
            #     'Attributes': attributes,
            #     'Lines': text_lines_content
            # }

            # # Here you can do whatever you need with text_block_data
            # # For example, you could yield this data to store it somewhere
            # yield text_block_data
            #     #yield scrapy.Request(url=link, callback=self.parse_pdf)
