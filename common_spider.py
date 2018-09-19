import requests
from fake_useragent import UserAgent
from retrying import retry
import queue
from spider_throttle import Throttle
from urllib.parse import urlparse,urljoin,urldefrag
from mongo_cache import MongoCache
from spider_utils import get_robots,extractor_url_lists,save_url
import threading
import os
import re
import random
MAX_DEP = 2 #下载深度


class SelfCommonSpider(object):
    def __init__(self,init_url):
        __ua = UserAgent()
        self.seed_url = init_url
        self.headers = {"User-Agent":__ua.random}
        self.throttle = Throttle()
        self.mongo = MongoCache()
        self.robots = get_robots(init_url)
        self.visited = {init_url:0}  #访问链接和访问深度
        self.product_queue = queue.Queue()  #生产者爬虫队列   (下载网页)
        self.product_queue.put(init_url)
        self.consumer_queue = queue.Queue() #消费者爬虫队列 （解析网址）
        self.deeppath = None


    @retry(stop_max_attempt_number=3)
    def retry_download(self,url_str,data,method,proxies):
        """
        通过装饰器封装重试下载模块，最多重试三次
        :param url_str: 下载网页的最终地址
        :param data: Post传输数据
        :param method: 下载方法 Post或者Get
        :param proxies: 设置过期时间
        :return:
        """
        if method == "POST":
            result = requests.post(url_str,data=data,headers=self.headers,proxies=proxies)
        else:
            result = requests.get(url_str,headers=self.headers,timeout=3,proxies=proxies)
        #断言函数  判断下载结果是否成功 如果条件为真，正常执行。 否则抛出异常
        assert result.status_code == 200
        return result.content

    def download(self,url_str,data=None,method="GET",proxies={}):
        """
        真正的下载类，代理模式
        """
        print("download url is ::::{}".format(url_str))
        try:
            result =self.retry_download(url_str,data,method,proxies)
        except requests.HTTPError as e: #异常处理尽量使用具体的异常
            print(e)
            result = None
        return result

    def nomalize(self,url_str):
        """
        补全下载链接
        :param url_str:
        :return:
        """
        real_url,_ = urldefrag(url_str)
        real_url = urljoin(self.seed_url,url_str)
        return real_url

    def product_spider(self):
        """
        从生产者队列中获取url，下载网页内容
        :return:
        """
        while True:
            url_str = self.product_queue.get()
            if self.robots.can_fetch(self.headers,url_str):
                self.deeppath = self.visited[url_str]
                if self.deeppath < MAX_DEP:
                    #执行限速下载
                    self.throttle.wait_url(url_str)
                    result = self.download(url_str,)
                    if result is not None:
                        self.consumer_queue.put(result)
                else:
                    print("roots.txt 禁止下载:", url_str)

    def consumer_spider(self):
        """
        从消费者队列中获取结果，进行解析url放入。
        :return:
        """
        while True:
            result = self.consumer_queue.get()
            save_url(result)
            url_str_list = extractor_url_lists(result)
            #筛选与mongodb有关的链接
            filter_urls = [link for link in url_str_list if re.search("/(mongodb)",link)]
            for url_str in filter_urls:
                real_url_str = self.nomalize(url_str)
                if real_url_str not in self.visited:
                    self.visited[real_url_str] = self.deeppath + 1
                    self.product_queue.put(real_url_str)

    def run(self,m=1,n=1):
        for i in range(m):
            p = threading.Thread(target=self.product_spider)
            p.start()
        for i in range(n):
            c = threading.Thread(target=self.consumer_spider)
            c.start()


if __name__ == "__main__":
    common = SelfCommonSpider('http://www.runoob.com/mongodb/mongodb-tutorial.html')
    common.run(2,2)