from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("JsonToParquet") \
        .master("local[*]") \
        .getOrCreate()
    
    # 1. 读取笨重的 JSON
    input_path = "hdfs://localhost:9000/input/movies/bupt_movie_lines.json"
    print("正在读取 JSON 数据...")
    df = spark.read.json(input_path)
    
    # 2. 保存为 Parquet (二进制列式存储)
    output_path = "hdfs://localhost:9000/input/movies/bupt_movie.parquet"
    print(f"正在转换并保存到: {output_path}")
    
    # mode("overwrite") 表示如果存在就覆盖
    df.write.mode("overwrite").parquet(output_path)
    
    print("转换完成！")
    spark.stop()

if __name__ == "__main__":
    main()