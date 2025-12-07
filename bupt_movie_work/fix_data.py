from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

def main():
    spark = SparkSession.builder \
        .appName("FixData") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
    # 1. 重新读取最原始的 JSON Lines (确保这个文件是好的)
    input_json = "hdfs://localhost:9000/input/movies/bupt_movie_lines.json"
    df = spark.read.json(input_json)
    
    print("原始 Schema:")
    df.printSchema()
    
    # 2. 强制保存为 Parquet
    output_parquet = "hdfs://localhost:9000/input/movies/bupt_movie_fixed.parquet"
    
    print("正在写入 Fixed Parquet...")
    df.write.mode("overwrite").parquet(output_parquet)
    print("写入完成。")
    spark.stop()

if __name__ == "__main__":
    main()