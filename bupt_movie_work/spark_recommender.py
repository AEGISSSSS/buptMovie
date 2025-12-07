# /usr/local/hadoop/bupt_movie_work/spark_recommender.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, year, desc

def main():
    # 1. 初始化 Spark 会话
    print("正在启动 Spark 引擎，请稍候...")
    spark = SparkSession.builder \
        .appName("MovieDynamicRecommender") \
        .master("local[*]") \
        .getOrCreate()
    
    # 减少日志输出，只显示错误
    spark.sparkContext.setLogLevel("ERROR")

    # 2. 读取数据 (确保读取的是修复后的 Parquet 文件)
    input_path = "hdfs://localhost:9000/input/movies/bupt_movie_fixed.parquet"
    print(f"正在加载数据: {input_path}")
    
    # 读取 Parquet
    df = spark.read.parquet(input_path)

    # 3. 数据预处理
    movies_df = df.select(
        col("title"),
        col("original_title"),
        col("vote_average"),
        col("genres"),
        col("production_countries"),
        col("release_date"),
        year(col("release_date")).alias("year")
    )

    # 4. 【关键步骤】预热内存
    movies_df.cache()
    
    print("正在预热内存（第一次加载可能需要几秒钟）...")
    total_count = movies_df.count()
    print(f"========================================")
    print(f"数据加载完成！内存中共有 {total_count} 部电影。")
    print(f"========================================")

    # 5. 进入交互式查询循环
    while True:
        # 打印菜单头
        print("\n" + "="*40)
        print(" 请输入筛选条件 (不想搜的项直接回车跳过) ")
        print(" 输入 'exit' 退出程序")
        print("="*40)
        sys.stdout.flush() # 强制刷新，防止菜单不显示

        try:
            # --- 步骤 1: 体裁 ---
            print("1/3 请输入体裁 (如 '剧情'): ", end='', flush=True)
            in_genre = sys.stdin.readline().strip()
            
            if in_genre.lower() == 'exit':
                print("正在退出...")
                break
            
            # --- 步骤 2: 国家 ---
            print("2/3 请输入国家 (如 'United States'): ", end='', flush=True)
            in_country = sys.stdin.readline().strip()

            # --- 步骤 3: 年份 ---
            print("3/3 请输入年份 (如 '2025'): ", end='', flush=True)
            in_year = sys.stdin.readline().strip()
            
            # 反馈用户刚才输了什么
            print(f"\n>>> 正在寻找: [体裁:{in_genre or '不限'}] + [国家:{in_country or '不限'}] + [年份:{in_year or '不限'}] ...")
            
            # 开始构建动态查询
            filtered_df = movies_df
            
            # 条件 1: 体裁
            if in_genre:
                filtered_df = filtered_df.filter(array_contains(col("genres"), in_genre))
                
            # 条件 2: 国家 (模糊匹配)
            if in_country:
                filtered_df = filtered_df.filter(
                    col("production_countries").cast("string").contains(in_country)
                )

            # 条件 3: 年份
            if in_year:
                filtered_df = filtered_df.filter(col("year") == int(in_year))

            # 执行查询
            results = filtered_df.orderBy(desc("vote_average")).limit(10).collect()

            # 展示结果
            if not results:
                print(">>> 很遗憾，未找到符合条件的电影。")
            else:
                print(f">>> 为您找到 {len(results)} 部热门电影:")
                print(f"{'评分':<6} | {'年份':<6} | {'电影名'}")
                print("-" * 50)
                for row in results:
                    print(f"{row['vote_average']:<6} | {row['year']:<6} | {row['title']}")
                    
        except Exception as e:
            print(f"发生错误: {e}")
            import traceback
            traceback.print_exc()

    spark.stop()

if __name__ == "__main__":
    main()