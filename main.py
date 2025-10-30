import scrapy
import json
import logging
import re
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging

# 配置日志
configure_logging(install_root_handler=False)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[logging.FileHandler('movie_spider.log'), logging.StreamHandler()]
)

class MovieSpider(scrapy.Spider):
    name = 'movie_genre_spider'
    
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'ROBOTSTXT_OBEY': False,
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self, *args, **kwargs):
        super(MovieSpider, self).__init__(*args, **kwargs)
        self.all_movies = []
        self.output_dir = "movie_data_with_genres"
        self.all_genres = set()  # 收集所有出现的类型
        import os
        os.makedirs(self.output_dir, exist_ok=True)

    def start_requests(self):
        urls = ['https://movie.douban.com/top250']
        
        for url in urls:
            yield scrapy.Request(
                url=url, 
                callback=self.parse_list_page,
                headers={
                    'Referer': 'https://movie.douban.com/',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                }
            )

    def parse_list_page(self, response):
        """解析电影列表页"""
        movies = response.css('.item')
        
        for movie in movies:
            detail_url = movie.css('.hd a::attr(href)').get()
            if detail_url:
                # 先从列表页提取基本信息，包括可能的类型
                basic_info = {
                    'title': movie.css('.title::text').get(),
                    'rating': movie.css('.rating_num::text').get(),
                    'quote': movie.css('.inq::text').get(),
                }
                
                yield scrapy.Request(
                    url=detail_url,
                    callback=self.parse_movie_detail,
                    headers={
                        'Referer': response.url,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                    },
                    meta={'basic_info': basic_info}
                )
        
        # 处理分页
        next_page = response.css('.next a::attr(href)').get()
        if next_page:
            next_url = response.urljoin(next_page)
            self.logger.info(f"转到下一页: {next_url}")
            yield scrapy.Request(next_url, callback=self.parse_list_page)

    def parse_movie_detail(self, response):
        """解析电影详情页，重点提取类型信息"""
        movie_item = {}
        
        # 合并基本信息
        basic_info = response.meta.get('basic_info', {})
        movie_item.update(basic_info)
        
        # 提取标题和年份
        title_text = response.css('h1 span::text').getall()
        if title_text:
            movie_item['title'] = title_text[0].strip()
            if len(title_text) > 1:
                movie_item['original_title'] = title_text[1].strip()
        
        # 提取年份
        year_match = re.search(r'\((\d{4})\)', response.css('h1').get() or "")
        movie_item['year'] = year_match.group(1) if year_match else ""
        
        # 重点：提取类型信息 - 多种方法确保获取完整
        genres = self.extract_genres(response)
        movie_item['genres'] = genres
        
        # 添加到全局类型集合
        self.all_genres.update(genres)
        
        # 其他信息提取
        movie_item.update({
            'rating': response.css('.rating_num::text').get() or movie_item.get('rating'),
            'rating_people': response.css('.rating_people span::text').get(),
            'directors': response.css('[rel="v:directedBy"]::text').getall(),
            'actors': response.css('.actor .attrs a::text').getall()[:10],  # 前10个演员
            'summary': response.css('[property="v:summary"]::text').get("").strip(),
            'runtime': response.css('[property="v:runtime"]::text').get(),
            'release_dates': response.css('[property="v:initialReleaseDate"]::text').getall(),
            'url': response.url,
            'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # 从信息区域提取更多类型相关信息
        info_html = response.css('#info').get() or ""
        movie_item.update(self.extract_additional_info(info_html))
        
        self.logger.info(f"爬取电影: {movie_item.get('title')} - 类型: {', '.join(genres)}")
        self.all_movies.append(movie_item)
        
        # 每爬取10部电影显示一次进度
        if len(self.all_movies) % 10 == 0:
            self.logger.info(f"已爬取 {len(self.all_movies)} 部电影，发现 {len(self.all_genres)} 种类型")

    def extract_genres(self, response):
        """提取电影类型 - 使用多种方法确保完整性"""
        genres = []
        
        # 方法1: 使用属性选择器（最可靠的方法）
        genres = response.css('[property="v:genre"]::text').getall()
        
        # 方法2: 从信息区域提取
        if not genres:
            info_text = response.css('#info').get()
            if info_text:
                # 查找"类型:"后面的内容
                genre_match = re.search(r'类型:</span>(.+?)<br>', info_text)
                if genre_match:
                    genre_text = genre_match.group(1)
                    # 提取链接文本
                    genre_links = re.findall(r'<a[^>]*>([^<]+)</a>', genre_text)
                    genres = [g.strip() for g in genre_links if g.strip()]
        
        # 方法3: 从标签区域提取相关类型
        if not genres:
            tags = response.css('.tags-body a::text').getall()
            # 从标签中筛选出可能是类型的词
            potential_genres = self.filter_potential_genres(tags)
            if potential_genres:
                genres = potential_genres
        
        # 清理和去重
        genres = [g.strip() for g in genres if g.strip()]
        return list(set(genres))

    def filter_potential_genres(self, tags):
        """从标签中筛选出可能的类型"""
        # 常见的电影类型关键词
        genre_keywords = {
            '剧情', '喜剧', '动作', '爱情', '科幻', '恐怖', '悬疑', '惊悚', 
            '犯罪', '冒险', '奇幻', '家庭', '动画', '传记', '历史', '战争',
            '歌舞', '音乐', '武侠', '古装', '运动', '黑色电影', '短片', 
            '纪录片', '脱口秀', '真人秀', '情色', '灾难', '西部', '鬼怪'
        }
        
        found_genres = []
        for tag in tags:
            if tag in genre_keywords:
                found_genres.append(tag)
        
        return found_genres

    def extract_additional_info(self, info_html):
        """从信息区域提取额外信息"""
        info = {}
        
        if not info_html:
            return info
        
        # 提取制片国家/地区
        country_match = re.search(r'制片国家/地区:</span>\s*(.+?)<br>', info_html)
        if country_match:
            countries = country_match.group(1).split(' / ')
            info['countries'] = [c.strip() for c in countries if c.strip()]
        
        # 提取语言
        language_match = re.search(r'语言:</span>\s*(.+?)<br>', info_html)
        if language_match:
            languages = language_match.group(1).split(' / ')
            info['languages'] = [l.strip() for l in languages if l.strip()]
        
        # 提取又名
        aka_match = re.search(r'又名:</span>\s*(.+?)<br>', info_html)
        if aka_match:
            aka_titles = aka_match.group(1).split(' / ')
            info['aka_titles'] = [a.strip() for a in aka_titles if a.strip()]
        
        # IMDb编号
        imdb_match = re.search(r'IMDb:</span>\s*(tt\d+)\s*<br>', info_html)
        if imdb_match:
            info['imdb_id'] = imdb_match.group(1)
        
        return info

    def closed(self, reason):
        """爬虫关闭时保存数据"""
        if self.all_movies:
            self.save_data()
        
        # 保存类型统计
        self.save_genre_stats()

    def save_data(self):
        """保存电影数据"""
        filename = f"movies_with_genres_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        local_path = f"{self.output_dir}/{filename}"
        
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(self.all_movies, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"成功保存 {len(self.all_movies)} 部电影数据到: {local_path}")
        self.show_stats()

    def save_genre_stats(self):
        """保存类型统计信息"""
        genre_stats = {}
        
        for genre in self.all_genres:
            count = sum(1 for movie in self.all_movies if genre in movie.get('genres', []))
            genre_stats[genre] = count
        
        # 按数量排序
        genre_stats = dict(sorted(genre_stats.items(), key=lambda x: x[1], reverse=True))
        
        stats_file = f"{self.output_dir}/genre_statistics.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(genre_stats, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"类型统计已保存到: {stats_file}")

    def show_stats(self):
        """显示统计信息"""
        if not self.all_movies:
            return
        
        # 类型统计
        genre_counter = {}
        for movie in self.all_movies:
            for genre in movie.get('genres', []):
                genre_counter[genre] = genre_counter.get(genre, 0) + 1
        
        # 按数量排序
        sorted_genres = sorted(genre_counter.items(), key=lambda x: x[1], reverse=True)
        
        self.logger.info("=" * 60)
        self.logger.info("电影类型统计:")
        self.logger.info(f"总电影数: {len(self.all_movies)}")
        self.logger.info(f"发现类型数: {len(self.all_genres)}")
        self.logger.info("类型分布:")
        for genre, count in sorted_genres[:15]:  # 显示前15个类型
            percentage = (count / len(self.all_movies)) * 100
            self.logger.info(f"  {genre}: {count}部 ({percentage:.1f}%)")
        self.logger.info("=" * 60)

def run_spider():
    """运行爬虫"""
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    })
    
    process.crawl(MovieSpider)
    process.start()

if __name__ == "__main__":
    print("开始爬取豆瓣电影（重点提取类型信息）...")
    run_spider()