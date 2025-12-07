from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, collect_list, lit, concat

def main():
    spark = SparkSession.builder.appName("UserCF_Batch").master("local[*]").getOrCreate()
    
    # 读取评分数据
    df = spark.read.json("hdfs://localhost:9000/input/movies/user_ratings.json")
    
    # 1. 找到所有用户
    all_users = [row['user_id'] for row in df.select("user_id").distinct().collect()]
    # 为了演示快一点，我们只算前20个用户 + 你的测试用户 999
    target_users = all_users[:20]
    if 999 not in target_users: target_users.append(999)

    final_results = []
    
    # 2. 只有 >= 4分才算喜欢
    high_rated = df.filter(col("rating") >= 4).cache()

    print(f"开始为 {len(target_users)} 位用户生成推荐...")

    for uid in target_users:
        # A. 找出我喜欢的
        my_likes = high_rated.filter(col("user_id") == uid)
        my_titles = [r['movie_title'] for r in my_likes.collect()]
        
        if not my_titles: continue

        # B. 找相似用户 (Self Join)
        others = high_rated.filter(col("user_id") != uid)
        
        # 共同喜欢的
        common = my_likes.alias("me").join(others.alias("other"), 
            col("me.movie_title") == col("other.movie_title")) \
            .groupBy(col("other.user_id").alias("sim_id")) \
            .count().orderBy(desc("count"))
            
        # 拿最相似的那个人
        top_sim = common.limit(1).collect()
        if not top_sim: continue
        
        sim_user_id = top_sim[0]['sim_id']
        
        # C. 找推荐 (他看过但我没看过的)
        sim_user_likes = high_rated.filter(col("user_id") == sim_user_id)
        
        # 简单的差集逻辑
        recs = sim_user_likes.filter(~col("movie_title").isin(my_titles)).limit(5).collect()
        
        for r in recs:
            # 存入列表: user_id, movie, reason
            final_results.append((uid, r['movie_title'], f"Based on SimUser {sim_user_id}"))

    # 3. 保存结果
    res_df = spark.createDataFrame(final_results, ["user_id", "movie", "reason"])
    # 存为 CSV
    res_df.write.mode("overwrite").csv("hdfs://localhost:9000/output/export_user_cf")
    print("用户协同过滤计算完成！")
    spark.stop()

if __name__ == "__main__":
    main()