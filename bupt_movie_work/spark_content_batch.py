from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, desc, array_contains, lit, when, concat_ws

def main():
    spark = SparkSession.builder.appName("Content_Batch").master("local[*]").getOrCreate()
    
    movies_df = spark.read.parquet("hdfs://localhost:9000/input/movies/bupt_movie_fixed.parquet")
    ratings_df = spark.read.json("hdfs://localhost:9000/input/movies/user_ratings.json")
    
    # 同样只算部分用户
    target_users = [999] + [i for i in range(1, 11)] 

    final_results = []

    for uid in target_users:
        # 1. 构建画像
        my_high = ratings_df.filter((col("user_id") == uid) & (col("rating") >= 4))
        my_genres = my_high.join(movies_df, my_high.movie_title == movies_df.title) \
            .select(explode("genres").alias("genre")) \
            .groupBy("genre").count().orderBy(desc("count")).limit(2).collect()
            
        if not my_genres: continue
        top_gs = [r['genre'] for r in my_genres] # ['剧情', '犯罪']
        
        # 2. 找电影 (包含这些标签且分高)
        # 简单逻辑：只要包含第一喜欢的标签，且评分>7
        recs = movies_df.filter(array_contains("genres", top_gs[0])) \
            .filter(col("vote_average") > 7.5) \
            .orderBy(desc("vote_average")).limit(5).collect()
            
        for r in recs:
            tags = ",".join(top_gs)
            final_results.append((uid, r['title'], tags))

    res_df = spark.createDataFrame(final_results, ["user_id", "movie", "tags"])
    res_df.write.mode("overwrite").csv("hdfs://localhost:9000/output/export_content_cf")
    print("内容推荐计算完成！")
    spark.stop()

if __name__ == "__main__":
    main()