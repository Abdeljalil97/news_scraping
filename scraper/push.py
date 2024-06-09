import redis 

redisclient = redis.from_url('redis://127.0.0.1:6379')
with open('xml_url_links_SNP2532889X.txt', 'r',encoding="utf-8") as f:
    pdf_urls = f.readlines()
    print(pdf_urls)
    #pdf_urls = [url.replace('\n','').strip for url in pdf_urls]
#print(pdf_urls)
for url in pdf_urls:
    redisclient.lpush('data_queue:mark3', url.replace('\n','').strip())
    