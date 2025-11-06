import json
import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns

class GenreAnalyzer:
    def __init__(self, data_file):
        self.data_file = data_file
        self.movies = self.load_data()
    
    def load_data(self):
        """加载电影数据"""
        with open(self.data_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def analyze_genre_distribution(self):
        """分析类型分布"""
        all_genres = []
        for movie in self.movies:
            all_genres.extend(movie.get('genres', []))
        
        genre_counter = Counter(all_genres)
        
        print("电影类型分布统计:")
        print("=" * 50)
        total_movies = len(self.movies)
        for genre, count in genre_counter.most_common():
            percentage = (count / total_movies) * 100
            print(f"{genre:<8}: {count:>3}部 ({percentage:>5.1f}%)")
        
        return genre_counter
    
    def find_movies_by_genre(self, genre):
        """根据类型查找电影"""
        matching_movies = []
        for movie in self.movies:
            if genre in movie.get('genres', []):
                matching_movies.append({
                    'title': movie.get('title'),
                    'rating': movie.get('rating'),
                    'year': movie.get('year'),
                    'genres': movie.get('genres', [])
                })
        
        # 按评分排序
        matching_movies.sort(key=lambda x: float(x.get('rating', 0)), reverse=True)
        return matching_movies
    
    def analyze_genre_combinations(self):
        """分析类型组合"""
        genre_combinations = Counter()
        
        for movie in self.movies:
            genres = movie.get('genres', [])
            if len(genres) >= 2:
                # 记录所有类型组合
                combination = '+'.join(sorted(genres))
                genre_combinations[combination] += 1
        
        print("\n热门类型组合:")
        print("=" * 50)
        for combo, count in genre_combinations.most_common(10):
            print(f"{combo}: {count}部")
        
        return genre_combinations
    
    def create_genre_matrix(self):
        """创建类型共现矩阵"""
        all_genres = set()
        for movie in self.movies:
            all_genres.update(movie.get('genres', []))
        
        all_genres = sorted(all_genres)
        genre_matrix = pd.DataFrame(0, index=all_genres, columns=all_genres)
        
        # 填充共现矩阵
        for movie in self.movies:
            genres = movie.get('genres', [])
            for i, genre1 in enumerate(genres):
                for genre2 in genres[i+1:]:
                    genre_matrix.loc[genre1, genre2] += 1
                    genre_matrix.loc[genre2, genre1] += 1
        
        return genre_matrix
    
    def plot_genre_distribution(self):
        """绘制类型分布图"""
        genre_counter = self.analyze_genre_distribution()
        
        # 准备数据
        genres = [item[0] for item in genre_counter.most_common(15)]
        counts = [item[1] for item in genre_counter.most_common(15)]
        
        # 绘制条形图
        plt.figure(figsize=(12, 8))
        sns.barplot(x=counts, y=genres, palette='viridis')
        plt.title('电影类型分布 (Top 15)')
        plt.xlabel('电影数量')
        plt.tight_layout()
        plt.savefig('genre_distribution.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def export_genre_data(self, output_file):
        """导出类型数据"""
        genre_data = {
            'movies_count': len(self.movies),
            'genres_found': list(set(g for movie in self.movies for g in movie.get('genres', []))),
            'genre_stats': dict(self.analyze_genre_distribution()),
            'genre_combinations': dict(self.analyze_genre_combinations().most_common(20))
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(genre_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n类型数据已导出到: {output_file}")

# 使用示例
if __name__ == "__main__":
    # 替换为实际的数据文件路径
    data_file = "movie_data_with_genres/movies_with_genres_最新.json"
    
    try:
        analyzer = GenreAnalyzer(data_file)
        
        print("电影类型分析报告")
        print("=" * 60)
        
        # 分析类型分布
        analyzer.analyze_genre_distribution()
        
        # 分析类型组合
        analyzer.analyze_genre_combinations()
        
        # 查找特定类型的电影
        print("\n查找'科幻'类型的高分电影:")
        sci_fi_movies = analyzer.find_movies_by_genre('科幻')
        for i, movie in enumerate(sci_fi_movies[:5], 1):
            print(f"{i}. {movie['title']} ({movie['year']}) - 评分: {movie['rating']}")
        
        # 导出数据
        analyzer.export_genre_data('genre_analysis_report.json')
        
        # 绘制图表（可选）
        # analyzer.plot_genre_distribution()
        
    except FileNotFoundError:
        print(f"数据文件未找到: {data_file}")
    except Exception as e:
        print(f"分析过程中出错: {e}")