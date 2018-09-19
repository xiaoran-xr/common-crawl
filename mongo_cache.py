import pickle   #序列化对象
import zlib     #压缩文件
from datetime import datetime,timedelta
from pymongo import MongoClient
from bson.binary import Binary #mongo存储二进制的存储要求

class MongoCache(object):
    def __init__(self,client=None,expires=timedelta(days=30)):
        """
        初始化
        :param client:
        :param expires:
        """
        self.client = MongoClient('localhost',27017)
        self.db = self.client.cache
        #创建webpage数据库
        self.web_page = self.db.webpage
        #设置索引，设置超时时间     total_second() 将30天转化为秒。
        self.db.webpage.create_index('timestamp',expireAfterSeconds=expires.total_seconds())


    def __setitem__(self, key, value):
        """
        向数据库添加一条缓存（数据）
        :param key: 缓存关键字
        :param value: 缓存内容
        :return:
        """
        #将数据使用pickl序列化，再使用zlib压缩并且转换为Binary类型格式。使用0时区时间
        record = {"result":Binary(zlib.compress(pickle.dumps(value))),"timestamp":datetime.utcnow()}
        self.db.webpage.update({"_id":key},{"$set":record},upsert=True)

    def __getitem__(self, item):
        """
        将缓存数据按照item作为key取出（key仍然是下载的url）
        :param item: 根据_id的值取数据
        :return:
        """
        record = self.db.webpage.find_one({"_id":item})
        if record:
            return pickle.loads(zlib.decompress(record['result']))
        else:
            return None

    def __contains__(self, item):
        try:
            self[item]
        except KeyError:
            return False
        else:
            return True

    def clear(self):
        """
        清空数据库
        :return:
        """
        self.db.webpage.drop()