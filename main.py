# 使用Scrapy框架
import scrapy
import json
from hdfs import InsecureClient
from scrapy.selector import Selector


class MovieSpider(scrapy.Spider):
    name = 'movie_spider'

    def start_requests(self):
        # 目标网站列表
        urls = [
            'https://movie.douban.com/top250',
            # 其他电影网站
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # 解析电影信息
        movies = response.css('.item')
        for movie in movies:
            yield {
                'title': movie.css('.title::text').get(),
                'rating': movie.css('.rating_num::text').get(),
                'description': movie.css('.inq::text').get(),
                # 其他字段...
            }



# 连接到HDFS
client = InsecureClient('http://localhost:9000', user='hadoop')

def save_to_hdfs(data, filename):
    with client.write(f'/movie_data/{filename}', encoding='utf-8') as writer:
        json.dump(data, writer)