#!/usr/bin/env python3
import subprocess
import json
import os
from datetime import datetime

def list_hdfs_files(hdfs_path):
    """列出HDFS目录中的文件"""
    try:
        result = subprocess.run([
            '/usr/local/hadoop/bin/hdfs', 'dfs', '-ls', hdfs_path
        ], capture_output=True, text=True, check=True)
        
        files = []
        for line in result.stdout.split('\n'):
            if line and not line.startswith('Found'):
                parts = line.split()
                if len(parts) >= 8:
                    file_info = {
                        'permissions': parts[0],
                        'owner': parts[2],
                        'group': parts[3],
                        'size': parts[4],
                        'date': f"{parts[5]} {parts[6]}",
                        'name': parts[7]
                    }
                    files.append(file_info)
        
        return files
    except subprocess.CalledProcessError as e:
        print(f"列出文件失败: {e}")
        return []

def download_from_hdfs(hdfs_file, local_path):
    """从HDFS下载文件"""
    try:
        # 确保本地目录存在
        os.makedirs(os.path.dirname(local_path) if os.path.dirname(local_path) else '.', exist_ok=True)
        
        # 下载文件
        result = subprocess.run([
            '/usr/local/hadoop/bin/hdfs', 'dfs', '-get',
            hdfs_file, local_path
        ], capture_output=True, text=True, check=True)
        
        print(f"✓ 下载成功: {hdfs_file} -> {local_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ 下载失败: {e}")
        if e.stderr:
            print(f"错误信息: {e.stderr}")
        return False

def download_latest_movie_data():
    """下载最新的电影数据文件"""
    print("HDFS文件下载工具")
    print("=" * 50)
    
    # 列出HDFS上的文件
    files = list_hdfs_files('/movie_data')
    
    if not files:
        print("未找到任何文件")
        return
    
    # 显示文件列表
    print("HDFS上的电影数据文件:")
    print("-" * 50)
    for i, file_info in enumerate(files, 1):
        print(f"{i}. {file_info['name']}")
        print(f"   大小: {file_info['size']} bytes, 修改时间: {file_info['date']}")
        print()
    
    # 选择要下载的文件
    try:
        choice = input("请输入要下载的文件编号 (默认下载最新文件): ").strip()
        if choice == "":
            # 下载最新的文件（列表中的第一个）
            selected_file = files[0]['name']
        else:
            selected_file = files[int(choice) - 1]['name']
        
        # 生成本地文件名
        filename = os.path.basename(selected_file)
        local_dir = "downloaded_movie_data"
        local_path = os.path.join(local_dir, filename)
        
        # 下载文件
        if download_from_hdfs(selected_file, local_path):
            # 显示文件内容预览
            preview_downloaded_file(local_path)
            
    except (ValueError, IndexError):
        print("无效的选择")
    except KeyboardInterrupt:
        print("\n下载取消")

def preview_downloaded_file(file_path):
    """预览下载的文件内容"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print("\n" + "=" * 50)
        print("文件内容预览:")
        print(f"总记录数: {len(data)}")
        print("\n前5条记录:")
        for i, movie in enumerate(data[:5], 1):
            print(f"{i}. {movie.get('title', 'N/A')} - 评分: {movie.get('rating', 'N/A')}")
        
        # 统计信息
        ratings = [float(movie['rating']) for movie in data if movie.get('rating')]
        if ratings:
            avg_rating = sum(ratings) / len(ratings)
            print(f"\n统计信息:")
            print(f"- 平均评分: {avg_rating:.2f}")
            print(f"- 最高评分: {max(ratings)}")
            print(f"- 最低评分: {min(ratings)}")
        
        print(f"\n完整文件已保存到: {file_path}")
        
    except Exception as e:
        print(f"预览文件时出错: {e}")

def download_all_movie_data():
    """下载所有电影数据文件"""
    local_dir = "downloaded_movie_data"
    os.makedirs(local_dir, exist_ok=True)
    
    files = list_hdfs_files('/movie_data')
    if not files:
        print("未找到任何文件")
        return
    
    success_count = 0
    for file_info in files:
        hdfs_path = file_info['name']
        filename = os.path.basename(hdfs_path)
        local_path = os.path.join(local_dir, filename)
        
        if download_from_hdfs(hdfs_path, local_path):
            success_count += 1
    
    print(f"\n下载完成: {success_count}/{len(files)} 个文件")
    print(f"文件保存到: {local_dir}")

if __name__ == "__main__":
    print("选择下载选项:")
    print("1. 交互式下载（选择文件）")
    print("2. 下载所有文件")
    print("3. 仅列出文件")
    
    choice = input("请输入选择 (默认1): ").strip()
    
    if choice == "2":
        download_all_movie_data()
    elif choice == "3":
        files = list_hdfs_files('/movie_data')
        for file_info in files:
            print(f"{file_info['name']} - {file_info['size']} bytes")
    else:
        download_latest_movie_data()