import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, desc, avg, array_contains, lit, when

def main():
    # 1. 初始化 Spark
    spark = SparkSession.builder \
        .appName("ContentBasedRecommender") \
        .master("local[*]") \
        .config("spark.driver.memory", "5g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # ==========================================
    # 数据加载
    # ==========================================
    print("正在加载数据...")
    
    # 1. 电影元数据 (Parquet 格式，包含 genres 数组)
    movies_df = spark.read.parquet("hdfs://localhost:9000/input/movies/bupt_movie_fixed.parquet")
    
    # 2. 用户评分数据 (JSON 格式)
    ratings_df = spark.read.json("hdfs://localhost:9000/input/movies/user_ratings.json")

    # 设定当前用户 (我们还是用之前的 999 号用户)
    MY_UID = 999
    print(f"当前分析用户 ID: {MY_UID}")

    # ==========================================
    # 核心算法 Step 1: 构建用户画像 (User Profile)
    # 算出该用户最喜欢的体裁是什么
    # ==========================================
    
    # 1.1 找出用户打过高分 (>=4) 的电影
    my_high_ratings = ratings_df.filter((col("user_id") == MY_UID) & (col("rating") >= 4))
    
    # 1.2 关联电影表，获取这些电影的 genres
    # 只要 title, genres
    my_liked_movies_info = my_high_ratings.join(movies_df, my_high_ratings.movie_title == movies_df.title) \
                                          .select(movies_df.title, movies_df.genres)

    # 1.3 炸裂 (Explode) genres 数组，统计体裁频次
    # 比如我看了一部 [喜剧, 动作]，这算 1次喜剧 + 1次动作
    genre_profile = my_liked_movies_info.select(explode(col("genres")).alias("genre")) \
        .groupBy("genre") \
        .agg(count("*").alias("weight")) \
        .orderBy(desc("weight"))

    # 收集前 3 个最喜欢的体裁
    top_genres_rows = genre_profile.limit(3).collect()
    
    if not top_genres_rows:
        print("用户画像构建失败：您还没给任何电影打过高分。")
        spark.stop()
        return

    top_genres_list = [row['genre'] for row in top_genres_rows]
    print("\n" + "="*50)
    print("【用户画像分析报告】")
    print(f"根据您的评分历史，您的核心口味是: {top_genres_list}")
    print("各体裁权重分布:")
    genre_profile.show()
    print("="*50)

    # ==========================================
    # 核心算法 Step 2: 基于画像召回电影
    # 找包含这些体裁、高分、且我没看过的电影
    # ==========================================
    
    print(f"\n>>> 正在为您寻找 {top_genres_list} 领域的佳作...")

    # 2.1 获取我看过的所有电影列表 (用于过滤)
    my_watched_list = [row['movie_title'] for row in ratings_df.filter(col("user_id") == MY_UID).select("movie_title").collect()]
    
    # 2.2 筛选候选集
    # 逻辑：
    # 1. 电影包含 top_genres_list 中的至少一个 (这里我们简化为：包含权重最高的那个即可，或者用 SQL 复杂逻辑)
    # 2. 电影评分不能太低 (例如 > 6.0)
    # 3. 排除我看过的
    
    # 这里我们使用一个评分机制：
    # 如果电影包含用户最喜欢的体裁(第1名)，得3分；包含第2名，得2分；包含第3名，得1分。
    # 最后按这个“匹配分”排序。

    # 为了在 DataFrame 里操作，我们需要用到 Spark SQL 的 array_contains
    candidate_df = movies_df.filter(col("vote_average") > 6.0)
    
    # 动态构建匹配得分 (Match Score)
    # 初始分数为 0
    match_score_expr = lit(0)
    
    # 权重最高的体裁加 100 分，第二高加 10 分，第三高加 1 分 (确保优先级)
    if len(top_genres_list) >= 1:
        g1 = top_genres_list[0]
        match_score_expr = match_score_expr + when(array_contains(col("genres"), g1), 100).otherwise(0)
        
    if len(top_genres_list) >= 2:
        g2 = top_genres_list[1]
        match_score_expr = match_score_expr + when(array_contains(col("genres"), g2), 10).otherwise(0)

    if len(top_genres_list) >= 3:
        g3 = top_genres_list[2]
        match_score_expr = match_score_expr + when(array_contains(col("genres"), g3), 1).otherwise(0)

    # 应用评分并排序
    recommendations = candidate_df.withColumn("match_score", match_score_expr) \
        .filter(col("match_score") > 0) \
        .orderBy(desc("match_score"), desc("vote_average")) \
        .select("title", "genres", "vote_average", "match_score")

    # 2.3 过滤掉已看过的 (在本地做过滤，因为 limit 之后数据量很小)
    # 或者使用 Anti Join (大数据量推荐做法)
    
    # 转换成 List 处理
    candidates = recommendations.take(100) # 先拿 100 个候选
    
    final_recs = []
    for row in candidates:
        if row['title'] not in my_watched_list:
            final_recs.append(row)
            if len(final_recs) >= 10: break

    # ==========================================
    # 结果展示
    # ==========================================
    print(f"\n★ 基于您的口味 {top_genres_list} 的个性化推荐:")
    print("-" * 60)
    print(f"{'匹配度':<8} | {'评分':<6} | {'体裁':<20} | {'电影名'}")
    print("-" * 60)
    
    for row in final_recs:
        # 解析匹配度
        score = row['match_score']
        level = "一般"
        if score >= 100: level = "完美"
        elif score >= 10: level = "优秀"
        
        genres_str = ",".join(row['genres'])[:20] # 截断一下防止太长
        print(f"{level:<8} | {row['vote_average']:<6} | {genres_str:<20} | {row['title']}")

    spark.stop()

if __name__ == "__main__":
    main()