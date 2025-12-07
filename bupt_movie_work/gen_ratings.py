import json
import random

# ================= 配置 =================
MOVIE_FILE = '/usr/local/hadoop/bupt_movie_work/data/bupt_movie_lines.json'
OUTPUT_FILE = '/usr/local/hadoop/bupt_movie_work/data/user_ratings.json'
USER_COUNT = 500       # 模拟500个用户
MIN_RATING_COUNT = 30  # 每个用户最少看30部
MAX_RATING_COUNT = 60  # 每个用户最多看60部
# =======================================

def load_movies():
    movies = []
    try:
        with open(MOVIE_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                # 我们只记录标题和体裁，方便造数据
                movies.append({
                    "title": data['title'], 
                    "genres": data.get('genres', [])
                })
    except Exception as e:
        print(f"读取电影文件失败: {e}")
    return movies

def generate():
    movies = load_movies()
    if not movies: return

    print(f"加载了 {len(movies)} 部电影。正在生成 500+ 用户的高密度评分数据...")
    
    ratings_data = []

    # -------------------------------------------------
    # 1. 生成普通大众用户 (ID 1 - 500)
    # -------------------------------------------------
    for uid in range(1, USER_COUNT + 1):
        # 随机看几十部
        seen_movies = random.sample(movies, k=random.randint(MIN_RATING_COUNT, MAX_RATING_COUNT))
        for m in seen_movies:
            # 评分概率：大部分给3-4分，少部分5分或1-2分
            score = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 30, 35, 20])[0]
            ratings_data.append({
                "user_id": uid,
                "movie_title": m['title'],
                "rating": score
            })

    # -------------------------------------------------
    # 2. 定制“我” (Target User ID: 999)
    # 假设我特别喜欢“剧情”片，凡是剧情片我都给 5 分
    # -------------------------------------------------
    my_favorites = [m['title'] for m in movies if '剧情' in m['genres']]
    # 我看了其中 15 部剧情片
    my_watched = random.sample(my_favorites, 15)
    
    for title in my_watched:
        ratings_data.append({"user_id": 999, "movie_title": title, "rating": 5})

    # -------------------------------------------------
    # 3. 定制“灵魂伴侣” (Soulmate User ID: 888)
    # 他和我看了完全一样的 15 部剧情片，且都给 5 分
    # 另外他还多看了几部神作（这是我们要推荐出来的！）
    # -------------------------------------------------
    # 3.1 共同喜好
    for title in my_watched:
        ratings_data.append({"user_id": 888, "movie_title": title, "rating": 5})
    
    # 3.2 他的独家推荐 (我没看过的)
    hidden_gems = ["肖申克的救赎", "霸王别姬", "教父", "泰坦尼克号"]
    for title in hidden_gems:
        # 确保这些不在我已经看的列表里，否则推荐不出来
        if title not in my_watched: 
            ratings_data.append({"user_id": 888, "movie_title": title, "rating": 5})

    # -------------------------------------------------
    # 保存文件
    # -------------------------------------------------
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for r in ratings_data:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

    print(f"生成完成！共生成 {len(ratings_data)} 条评分记录。")
    print(f"数据已保存至: {OUTPUT_FILE}")
    print("请记得运行: hdfs dfs -put -f ... 上传到 HDFS")

if __name__ == "__main__":
    generate()