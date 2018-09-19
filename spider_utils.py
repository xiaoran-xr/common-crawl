import random
from urllib import robotparser
from urllib.parse import urlparse,urljoin
import re

def random_str():
    base_str = "QWERTYUIOPLKJHGFDSAZXCVBNMmnbvcxzasdfghjklpoiuytrewq"
    return "".join([ random.choice(base_str) for i in range(32)])

def save_url(html_content):
    """
    存储下载内容,文件名为随机。
    :param html_content:
    :param url_str:
    :return:
    """
    file_path = "Download/" + random_str() + ".html"
    with open(file_path,'wb') as f:
        f.write(html_content)

def get_robots(url_str):
    """
    解析robots文件，看是否让爬取.返回True或者False
    :param url_str:
    :return:
    """
    robots = robotparser.RobotFileParser()
    robots_url = "http://" + urlparse(url_str).netloc
    robots.set_url(urljoin(robots_url, "robots.txt"))
    robots.read()
    return robots

def extractor_url_lists(result_in):
    """
    返回网页上所有的链接列表
    :param result_in:
    :return:
    """
    re_object = re.compile('<a[^>]+href=["\'](.*?)["\']',re.I)
    url_str_list = re_object.findall(str(result_in,encoding='utf-8'))
    return url_str_list