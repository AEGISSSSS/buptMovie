import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, collect_list, lit

def main():
    spark = SparkSession.builder \
        .appName("AdvancedUserCF") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 1. 读取评分数据
    print("正在加载用户评分数据...")
    df = spark.read.json("hdfs://localhost:9000/input/movies/user_ratings.json")
    
    # 假设当前登录用户是 999
    MY_UID = 999
    
    # ========================================================
    # 核心算法步骤
    # ========================================================

    # 【Step 1】只关注“喜欢”的数据
    # 过滤掉评分低于 4 分的记录。
    # 逻辑：只有大家都觉得好（>=4分），才能证明品味一致。
    high_rated_df = df.filter(col("rating") >= 4).select("user_id", "movie_title")
    
    # 缓存一下，后面要反复用
    high_rated_df.cache()

    # 【Step 2】获取“我”喜欢的所有电影
    my_likes_df = high_rated_df.filter(col("user_id") == MY_UID)
    # 把我看过的电影转成 List，方便后面做过滤（Anti-Join）
    my_watched_list = [row['movie_title'] for row in my_likes_df.collect()]
    
    print(f"\n当前用户 (ID: {MY_UID}) 喜欢的电影 ({len(my_watched_list)}部):")
    print(my_watched_list[:5], "...")

    # 【Step 3】寻找灵魂伴侣 (Self-Join)
    # 表A (我喜欢的) JOIN 表B (其他人喜欢的) ON 电影名
    others_likes_df = high_rated_df.filter(col("user_id") != MY_UID)
    
    # 这里的 join 意味着：这部电影我和他都给了高分
    common_interests = my_likes_df.alias("me") \
        .join(others_likes_df.alias("other"), col("me.movie_title") == col("other.movie_title")) \
        .select(col("other.user_id").alias("similar_user_id"), col("other.movie_title"))

    # 【Step 4】计算相似度分数
    # 相似度 = 共同好评的电影数量
    similarity_df = common_interests.groupBy("similar_user_id") \
        .agg(
            count("movie_title").alias("score"), # 共同喜欢的数量
            collect_list("movie_title").alias("common_movies") # 列出具体的片名
        ) \
        .orderBy(desc("score")) # 分数高的排前面

    # 展示最相似的前 5 个用户
    top_similar_users = similarity_df.limit(5).collect()
    
    print("\n" + "="*50)
    print("【为您匹配到的“灵魂画师” (相似用户)】")
    print("="*50)
    
    if not top_similar_users:
        print("暂无数据！可能您还没对任何电影打过高分。")
        spark.stop()
        return

    # 打印相似用户详情
    for row in top_similar_users:
        print(f"用户 ID: {row['similar_user_id']}")
        print(f"匹配度: 你们都对 {row['score']} 部同一电影给了好评")
        # 截取前3个展示
        print(f"共同喜好: {row['common_movies'][:3]}...") 
        print("-" * 30)

    # 【Step 5】基于相似用户的协同推荐
    # 逻辑：取最相似的那个用户 (Top 1)，看他给 5 分但我没看过的电影
    
    best_match_user = top_similar_users[0]['similar_user_id']
    print(f"\n>>> 正在根据最强匹配用户 (ID: {best_match_user}) 生成推荐片单...")

    # 找出这个大神喜欢的所有电影
    expert_likes_df = high_rated_df.filter(col("user_id") == best_match_user)
    
    # 过滤掉我已经看过的 (差集)
    # Spark SQL 写法: ExpertLikes - MyLikes
    # 使用 Left Anti Join: 保留左边(expert)有，但右边(my)没有的
    recommendations_df = expert_likes_df.join(
        my_likes_df, 
        expert_likes_df.movie_title == my_likes_df.movie_title, 
        "left_anti"
    ).select(expert_likes_df.movie_title)

    final_recs = recommendations_df.limit(10).collect()

    print(f"\n★ 猜你喜欢 (来自用户 {best_match_user} 的高分收藏):")
    print("-" * 50)
    for i, row in enumerate(final_recs):
        print(f"{i+1}. {row['movie_title']}")
    
    spark.stop()

if __name__ == "__main__":
    main()