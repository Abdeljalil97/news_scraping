# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo

class ScraperPipeline:
    def process_item(self, item, spider):
        return item
class MongoDBPipeline(object):

    def __init__(self,MONGODB_uri,MONGODB_DB):
        self.MONGODB_uri = "mongodb://localhost:27017/"
        # self.MONGODB_PORT = MONGODB_PORT
        self.MONGODB_DB = "data"
        connection = pymongo.MongoClient(
           self.MONGODB_uri
           
        )
        self.db = connection[self.MONGODB_DB]
       
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            #MONGODB_uri=crawler.settings.get('MONGODB_uri'),
            MONGODB_uri="mongodb://localhost:27017/",
            #
            #MONGODB_PORT=crawler.settings.get('MONGODB_PORT'),
            #MONGODB_DB=crawler.settings.get('MONGODB_DB'),
            MONGODB_DB="data",
           
        )
    def process_item(self, item, spider):
        #“School_Gender_Sport”
        collection = "SNP26120215"
        collection = self.db[collection]
        collection.insert_one(dict(item))
        #collection.insert_one(dict(item))
        # if collection.find_one({'numero_inserzione': item['numero_inserzione']}) is None:
        #     collection.insert_one(dict(item))
        # else :
        #     print(f"product with numero_inserzione: {item['numero_inserzione']} exist .")
        #     filter = { 'numero_inserzione': item['numero_inserzione'] }
        #     newvalues = { "$set": dict(item) }
        #     try:
        #         collection.update_one(filter,newvalues)
        #     except WriteError as e :
        #         print(f"error : {e}")
        #         pass
        # valid = True