# /usr/local/hadoop/bupt_movie_work/preprocess.py
import json

input_path = '/usr/local/hadoop/movie/bupt_movie.json'
output_path = '/usr/local/hadoop/bupt_movie_work/data/bupt_movie_lines.json'

print("开始转换数据格式...")
try:
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f) # 一次性加载整个JSON数组
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for movie in data:
            # 将每个电影对象转换成一行字符串，确保没有换行符
            json_line = json.dumps(movie, ensure_ascii=False)
            f.write(json_line + '\n')
            
    print(f"转换完成！数据已保存至: {output_path}")
    print(f"总共处理了 {len(data)} 部电影。")
    
except Exception as e:
    print(f"发生错误: {e}")