import scrapy
import json
import logging
import re
import random
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from urllib.parse import urlencode, quote

# 配置日志
configure_logging(install_root_handler=False)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[logging.FileHandler('movie_spider.log'), logging.StreamHandler()]
)

class NoDoubanMovieSpider(scrapy.Spider):
    name = 'no_douban_movie_spider'
    
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'INFO',
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'HTTPCACHE_ENABLED': True,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 3,
    }

    def __init__(self, *args, **kwargs):
        super(NoDoubanMovieSpider, self).__init__(*args, **kwargs)
        self.all_movies = []
        self.output_dir = "movie_data_no_douban"
        self.all_genres = set()
        self.target_count = 2000
        self.processed_urls = set()
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
        ]
        
        import os
        os.makedirs(self.output_dir, exist_ok=True)

    def start_requests(self):
        """从非豆瓣数据源开始爬取"""
        sources = [
            # 猫眼电影多个榜单
            self.maoyan_requests(),
            # 时光网电影
            self.mtime_requests(),
            # 其他电影网站
            self.other_movie_sites_requests(),
        ]
        
        for source_requests in sources:
            for request in source_requests:
                yield request

    def maoyan_requests(self):
        """猫眼电影请求 - 主要数据源"""
        base_url = "https://maoyan.com"
        
        # 猫眼多个榜单和分类
        requests = []
        
        # 热门电影
        for i in range(1, 21):  # 20页，每页30部
            url = f"{base_url}/films?showType=1&offset={(i-1)*30}"
            requests.append(
                scrapy.Request(
                    url=url,
                    callback=self.parse_maoyan_list,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': 'maoyan_hot', 'page': i}
                )
            )
        
        # 即将上映
        for i in range(1, 11):  # 10页
            url = f"{base_url}/films?showType=2&offset={(i-1)*30}"
            requests.append(
                scrapy.Request(
                    url=url,
                    callback=self.parse_maoyan_list,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': 'maoyan_coming', 'page': i}
                )
            )
        
        # 经典电影
        for i in range(1, 11):  # 10页
            url = f"{base_url}/films?showType=3&offset={(i-1)*30}"
            requests.append(
                scrapy.Request(
                    url=url,
                    callback=self.parse_maoyan_list,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': 'maoyan_classic', 'page': i}
                )
            )
        
        return requests

    def mtime_requests(self):
        """时光网电影请求"""
        base_url = "http://movie.mtime.com"
        requests = []
        
        # 时光网热门电影
        for i in range(1, 51):  # 50页
            url = f"{base_url}/hot/?page={i}"
            requests.append(
                scrapy.Request(
                    url=url,
                    callback=self.parse_mtime_list,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': 'mtime_hot', 'page': i}
                )
            )
        
        # 时光网最新电影
        for i in range(1, 26):  # 25页
            url = f"{base_url}/latest/?page={i}"
            requests.append(
                scrapy.Request(
                    url=url,
                    callback=self.parse_mtime_list,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': 'mtime_latest', 'page': i}
                )
            )
        
        return requests

    def other_movie_sites_requests(self):
        """其他电影网站请求"""
        requests = []
        
        # 电影天堂
        for i in range(1, 101):  # 100页，每页约25部电影
            url = f"https://www.dytt8.net/html/gndy/dyzz/list_23_{i}.html"
            requests.append(
                scrapy.Request(
                    url=url,
                    callback=self.parse_dytt_list,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': 'dytt', 'page': i}
                )
            )
        
        # 1905电影网
        for i in range(1, 51):  # 50页
            url = f"http://www.1905.com/list-p-catid-220-1-{i}.html"
            requests.append(
                scrapy.Request(
                    url=url,
                    callback=self.parse_1905_list,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': '1905', 'page': i}
                )
            )
        
        return requests

    def parse_maoyan_list(self, response):
        """解析猫眼电影列表页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        self.logger.info(f"解析猫眼列表页: {response.url}")
        
        # 提取电影链接
        movie_links = response.css('.movie-item a::attr(href)').getall()
        movie_links.extend(response.css('.channel-detail a::attr(href)').getall())
        
        for link in movie_links:
            if len(self.all_movies) >= self.target_count:
                break
                
            if link and link.startswith('/films/'):
                full_url = response.urljoin(link)
                if full_url not in self.processed_urls:
                    self.processed_urls.add(full_url)
                    yield scrapy.Request(
                        url=full_url,
                        callback=self.parse_maoyan_detail,
                        headers={'User-Agent': random.choice(self.user_agents)},
                        meta={'source': 'maoyan'}
                    )

    def parse_maoyan_detail(self, response):
        """解析猫眼电影详情页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        try:
            movie_item = {
                'title': response.css('.movie-brief-container h1::text').get('').strip(),
                'english_title': response.css('.movie-brief-container .ename::text').get('').strip(),
                'genres': response.css('.movie-brief-container li:first-child a::text').getall(),
                'release_date': response.css('.movie-brief-container li:last-child::text').get('').strip(),
                'rating': response.css('.index-left .star-num::text').get(''),
                'actors': response.css('.celebrity-group:last-child .celebrity-name::text').getall()[:10],
                'directors': response.css('.celebrity-group:first-child .celebrity-name::text').getall(),
                'summary': response.css('.dra::text').get('').strip(),
                'source': 'maoyan',
                'url': response.url,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 提取更多信息
            info_text = response.css('.movie-brief-container').get('')
            
            # 提取国家/地区
            country_match = re.search(r'国家/地区:</span>\s*(.+?)</li>', info_text)
            if country_match:
                movie_item['countries'] = [c.strip() for c in country_match.group(1).split('/')]
            
            # 提取时长
            duration_match = re.search(r'时长:</span>\s*(\d+)分钟', info_text)
            if duration_match:
                movie_item['duration'] = f"{duration_match.group(1)}分钟"
            
            # 清理数据
            movie_item = self.clean_movie_data(movie_item)
            self.save_movie_item(movie_item)
            
        except Exception as e:
            self.logger.error(f"解析猫眼详情页错误: {e}")

    def parse_mtime_list(self, response):
        """解析时光网列表页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        self.logger.info(f"解析时光网列表页: {response.url}")
        
        # 时光网电影链接提取（根据实际HTML结构调整）
        movie_links = response.css('.movie_list li a::attr(href)').getall()
        movie_links.extend(response.css('.pic a::attr(href)').getall())
        
        for link in movie_links:
            if len(self.all_movies) >= self.target_count:
                break
                
            if link and link not in self.processed_urls:
                self.processed_urls.add(link)
                full_url = response.urljoin(link)
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_mtime_detail,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': 'mtime'}
                )

    def parse_mtime_detail(self, response):
        """解析时光网电影详情页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        try:
            movie_item = {
                'title': response.css('h1::text').get('').strip(),
                'genres': response.css('.type_a a::text').getall(),
                'release_date': response.css('.pubdate::text').get('').strip(),
                'rating': response.css('.point span::text').get(''),
                'directors': response.css('.director a::text').getall(),
                'actors': response.css('.actor a::text').getall()[:10],
                'summary': response.css('.description::text').get('').strip(),
                'source': 'mtime',
                'url': response.url,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 提取更多信息
            info_text = response.css('.info').get('')
            
            # 提取国家/地区
            country_match = re.search(r'国家/地区:</span>\s*(.+?)</li>', info_text)
            if country_match:
                movie_item['countries'] = [c.strip() for c in country_match.group(1).split('/')]
            
            # 提取时长
            duration_match = re.search(r'片长:</span>\s*(\d+).*?分钟', info_text)
            if duration_match:
                movie_item['duration'] = f"{duration_match.group(1)}分钟"
            
            movie_item = self.clean_movie_data(movie_item)
            self.save_movie_item(movie_item)
            
        except Exception as e:
            self.logger.error(f"解析时光网详情页错误: {e}")

    def parse_dytt_list(self, response):
        """解析电影天堂列表页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        self.logger.info(f"解析电影天堂列表页: {response.url}")
        
        # 电影天堂链接提取
        movie_links = response.css('.co_content8 ul a::attr(href)').getall()
        
        for link in movie_links:
            if len(self.all_movies) >= self.target_count:
                break
                
            if link and link.startswith('/html/gndy/dyzz/'):
                full_url = response.urljoin(link)
                if full_url not in self.processed_urls:
                    self.processed_urls.add(full_url)
                    yield scrapy.Request(
                        url=full_url,
                        callback=self.parse_dytt_detail,
                        headers={'User-Agent': random.choice(self.user_agents)},
                        meta={'source': 'dytt'}
                    )

    def parse_dytt_detail(self, response):
        """解析电影天堂详情页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        try:
            title = response.css('h1::text').get('').strip()
            content = response.css('#Zoom').get('')
            
            movie_item = {
                'title': title,
                'source': 'dytt',
                'url': response.url,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 从内容中提取信息
            if content:
                # 提取英文名
                en_match = re.search(r'◎译　　名(.*?)<br>', content)
                if en_match:
                    movie_item['english_title'] = en_match.group(1).strip()
                
                # 提取年份
                year_match = re.search(r'◎年　　代(.*?)<br>', content)
                if year_match:
                    movie_item['year'] = year_match.group(1).strip()
                
                # 提取类型
                genre_match = re.search(r'◎类　　别(.*?)<br>', content)
                if genre_match:
                    genres = [g.strip() for g in genre_match.group(1).split('/')]
                    movie_item['genres'] = genres
                    self.all_genres.update(genres)
                
                # 提取国家
                country_match = re.search(r'◎产　　地(.*?)<br>', content)
                if country_match:
                    movie_item['countries'] = [c.strip() for c in country_match.group(1).split('/')]
                
                # 提取简介
                desc_match = re.search(r'◎简　　介<br><br>(.*?)</p>', content, re.DOTALL)
                if desc_match:
                    movie_item['summary'] = desc_match.group(1).strip()
            
            movie_item = self.clean_movie_data(movie_item)
            self.save_movie_item(movie_item)
            
        except Exception as e:
            self.logger.error(f"解析电影天堂详情页错误: {e}")

    def parse_1905_list(self, response):
        """解析1905电影网列表页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        self.logger.info(f"解析1905电影网列表页: {response.url}")
        
        movie_links = response.css('.pic-list li a::attr(href)').getall()
        
        for link in movie_links:
            if len(self.all_movies) >= self.target_count:
                break
                
            if link and link not in self.processed_urls:
                self.processed_urls.add(link)
                full_url = response.urljoin(link)
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_1905_detail,
                    headers={'User-Agent': random.choice(self.user_agents)},
                    meta={'source': '1905'}
                )

    def parse_1905_detail(self, response):
        """解析1905电影网详情页"""
        if len(self.all_movies) >= self.target_count:
            return
        
        try:
            movie_item = {
                'title': response.css('h1::text').get('').strip(),
                'genres': response.css('.type a::text').getall(),
                'release_date': response.css('.time::text').get('').strip(),
                'actors': response.css('.actor a::text').getall()[:10],
                'directors': response.css('.director a::text').getall(),
                'summary': response.css('.des::text').get('').strip(),
                'source': '1905',
                'url': response.url,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            movie_item = self.clean_movie_data(movie_item)
            self.save_movie_item(movie_item)
            
        except Exception as e:
            self.logger.error(f"解析1905详情页错误: {e}")

    def clean_movie_data(self, movie_item):
        """清理电影数据"""
        cleaned = {}
        for k, v in movie_item.items():
            if v is None:
                cleaned[k] = ""
            elif isinstance(v, list):
                cleaned[k] = [item.strip() for item in v if item and str(item).strip()]
            elif isinstance(v, str):
                cleaned[k] = v.strip()
            else:
                cleaned[k] = v
        
        # 确保必要字段
        if 'title' not in cleaned or not cleaned['title']:
            cleaned['title'] = '未知电影'
        
        # 处理类型数据
        if 'genres' in cleaned and cleaned['genres']:
            self.all_genres.update(cleaned['genres'])
        
        return cleaned

    def save_movie_item(self, movie_item):
        """保存单部电影数据"""
        if movie_item['title'] != '未知电影':
            self.all_movies.append(movie_item)
            
            if len(self.all_movies) % 10 == 0:
                self.logger.info(f"已爬取 {len(self.all_movies)}/{self.target_count} 部电影")
                
            if len(self.all_movies) >= self.target_count:
                self.logger.info(f"已达到目标数量 {self.target_count}，准备停止爬虫")
                self.crawler.engine.close_spider(self, 'target_reached')

    def closed(self, reason):
        """爬虫关闭时保存数据"""
        if self.all_movies:
            self.save_data()
            self.save_genre_stats()
        
        self.logger.info(f"爬虫结束原因: {reason}")
        self.logger.info(f"最终爬取电影数量: {len(self.all_movies)}")

    def save_data(self):
        """保存电影数据"""
        filename = f"movies_{len(self.all_movies)}_items_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = f"{self.output_dir}/{filename}"
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.all_movies, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"成功保存 {len(self.all_movies)} 部电影数据到: {filepath}")
        self.show_stats()

    def save_genre_stats(self):
        """保存类型统计"""
        genre_stats = {}
        for genre in self.all_genres:
            count = sum(1 for movie in self.all_movies if genre in movie.get('genres', []))
            genre_stats[genre] = count
        
        genre_stats = dict(sorted(genre_stats.items(), key=lambda x: x[1], reverse=True))
        
        stats_file = f"{self.output_dir}/genre_statistics.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(genre_stats, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"类型统计已保存到: {stats_file}")

    def show_stats(self):
        """显示统计信息"""
        if not self.all_movies:
            return
        
        source_counter = {}
        for movie in self.all_movies:
            source = movie.get('source', 'unknown')
            source_counter[source] = source_counter.get(source, 0) + 1
        
        genre_counter = {}
        for movie in self.all_movies:
            for genre in movie.get('genres', []):
                genre_counter[genre] = genre_counter.get(genre, 0) + 1
        
        sorted_genres = sorted(genre_counter.items(), key=lambda x: x[1], reverse=True)
        
        self.logger.info("=" * 60)
        self.logger.info("最终统计结果:")
        self.logger.info(f"总电影数: {len(self.all_movies)}")
        self.logger.info(f"数据来源分布: {source_counter}")
        self.logger.info(f"发现类型数: {len(self.all_genres)}")
        self.logger.info("热门类型分布:")
        for genre, count in sorted_genres[:15]:
            percentage = (count / len(self.all_movies)) * 100
            self.logger.info(f"  {genre}: {count}部 ({percentage:.1f}%)")
        self.logger.info("=" * 60)

def run_spider():
    """运行爬虫"""
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 3,
    })
    
    process.crawl(NoDoubanMovieSpider)
    process.start()

if __name__ == "__main__":
    print("开始从非豆瓣数据源爬取电影数据（目标：2000条）...")
    print("数据源包括：猫眼电影、时光网、电影天堂、1905电影网")
    run_spider()