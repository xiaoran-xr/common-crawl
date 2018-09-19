from mongo_cache import MongoCache
from urllib.parse import urlparse
from datetime import datetime
import time
import random


class Throttle(object):
    def __init__(self,delay=3):
        """
        默认延时时间为3秒,避免固定时间被发现为爬虫
        在延时时间后加入随机时间。
        :param delay:
        """
        self.delay = delay + random.random()
        self.mongo = MongoCache()

    def wait_url(self,url_str):
        """
        等待,判断上一次访问时的时间与本次访问的时间间隔 与设置的事件间隔大小
        :return:
        """
        domain_url = urlparse(url_str).netloc
        last_accessed = self.mongo[domain_url]
        if last_accessed is not None and self.delay > 0:
            sleep_interval = self.delay - (datetime.now() - last_accessed).seconds
            if sleep_interval > 0:
                time.sleep(sleep_interval)
        self.mongo[domain_url] = datetime.now()