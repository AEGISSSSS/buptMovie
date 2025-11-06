#!/usr/bin/env python3
import subprocess
import json
import os
from datetime import datetime

def upload_to_hdfs(local_file, hdfs_path):
    """使用HDFS命令行工具上传文件"""
    try:
        # 确保HDFS目录存在
        subprocess.run([
            '/usr/local/hadoop/bin/hdfs', 'dfs', '-mkdir', '-p', 
            os.path.dirname(hdfs_path)
        ], check=True)
        
        # 上传文件
        result = subprocess.run([
            '/usr/local/hadoop/bin/hdfs', 'dfs', '-put', 
            '-f', local_file, hdfs_path
        ], capture_output=True, text=True, check=True)
        
        print(f"✓ 成功上传到HDFS: {hdfs_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ HDFS上传失败: {e}")
        if e.stderr:
            print(f"错误信息: {e.stderr}")
        return False
    except Exception as e:
        print(f"✗ 上传过程出错: {e}")
        return False

def check_hdfs_status():
    """检查HDFS状态"""
    try:
        # 检查HDFS是否可访问
        result = subprocess.run([
            '/usr/local/hadoop/bin/hdfs', 'dfs', '-ls', '/'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ HDFS可访问")
            return True
        else:
            print("✗ HDFS不可访问")
            return False
            
    except Exception as e:
        print(f"✗ 检查HDFS状态失败: {e}")
        return False

def main():
    print("HDFS文件上传工具")
    print("=" * 50)
    
    # 检查HDFS状态
    if not check_hdfs_status():
        print("请确保HDFS服务正在运行")
        return
    
    # 查找最新的电影数据文件
    data_dir = "movie_data"
    if not os.path.exists(data_dir):
        print(f"数据目录不存在: {data_dir}")
        return
    
    json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    if not json_files:
        print("未找到JSON数据文件")
        return
    
    # 选择最新的文件
    latest_file = max(json_files, key=lambda f: os.path.getctime(os.path.join(data_dir, f)))
    local_path = os.path.join(data_dir, latest_file)
    
    print(f"找到数据文件: {latest_file}")
    
    # 读取文件信息
    with open(local_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"文件包含 {len(data)} 条电影记录")
    
    # 上传到HDFS
    hdfs_path = f"/movie_data/{latest_file}"
    if upload_to_hdfs(local_path, hdfs_path):
        print("上传完成!")
        
        # 验证上传
        try:
            result = subprocess.run([
                '/usr/local/hadoop/bin/hdfs', 'dfs', '-ls', hdfs_path
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"✓ 验证: 文件已存在于HDFS")
                
                # 显示文件大小
                result = subprocess.run([
                    '/usr/local/hadoop/bin/hdfs', 'dfs', '-du', hdfs_path
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    size_info = result.stdout.strip().split()[0]
                    print(f"✓ 文件大小: {size_info} 字节")
                    
        except Exception as e:
            print(f"验证失败: {e}")
    
    else:
        print("上传失败，请检查HDFS服务状态")

if __name__ == "__main__":
    main()