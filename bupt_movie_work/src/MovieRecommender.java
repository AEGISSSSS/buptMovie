import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class MovieRecommender {

    // 使用 Gson 解析 JSON
    private static final Gson gson = new Gson();

    // ============================================================
    // 任务 1: 根据体裁推荐 (Genre Recommendation)
    // 建立 "体裁 -> 电影列表" 的倒排索引
    // ============================================================
    
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                // 读取一行 JSON
                JsonObject movie = gson.fromJson(value.toString(), JsonObject.class);
                
                // 获取电影基本信息
                String title = movie.get("title").getAsString();
                double score = movie.has("vote_average") ? movie.get("vote_average").getAsDouble() : 0.0;
                String releaseDate = movie.has("release_date") ? movie.get("release_date").getAsString() : "Unknown";
                
                // 构建输出的值：标题 + 分数 (方便排序)
                // 格式: 评分::电影名::上映日期
                String movieInfo = score + "::" + title + "::" + releaseDate;

                // 遍历 genres 数组
                if (movie.has("genres")) {
                    JsonArray genres = movie.getAsJsonArray("genres");
                    for (JsonElement g : genres) {
                        String genreName = g.getAsString();
                        // 输出: Key=体裁, Value=电影信息
                        context.write(new Text(genreName), new Text(movieInfo));
                    }
                }
            } catch (Exception e) {
                // 忽略解析错误的行
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 存储该体裁下的所有电影
            List<String> movies = new ArrayList<>();
            
            for (Text val : values) {
                movies.add(val.toString());
            }

            // 简单的排序逻辑：按评分降序排列 (字符串分割后解析)
            // 这里为了演示简单，我们直接按字符串降序（评分在最前面）
            Collections.sort(movies, Collections.reverseOrder());

            // 拼接推荐结果，为了美观，我们只推荐评分最高的 10 部
            StringBuilder recommendation = new StringBuilder();
            recommendation.append("\n【为您推荐的 ").append(key.toString()).append(" 电影】:\n");
            
            int count = 0;
            for (String m : movies) {
                String[] parts = m.split("::");
                if (parts.length >= 2) {
                    recommendation.append(String.format("  ★ %s分 | %s (%s)\n", parts[0], parts[1], parts[2]));
                    count++;
                }
                if (count >= 10) break; // 只推荐 Top 10
            }

            context.write(key, new Text(recommendation.toString()));
        }
    }

    // ============================================================
    // 任务 2: 电影名字搜索 (Movie Search)
    // ============================================================

    public static class SearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        
        private String searchKeyword;

        @Override
        protected void setup(Context context) {
            // 从配置中获取用户搜索的关键词
            searchKeyword = context.getConfiguration().get("search.keyword", "").toLowerCase();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JsonObject movie = gson.fromJson(value.toString(), JsonObject.class);
                String title = movie.get("title").getAsString();
                
                // 如果标题包含搜索词 (忽略大小写)
                if (title.toLowerCase().contains(searchKeyword)) {
                    // 格式化输出详细介绍
                    StringBuilder info = new StringBuilder();
                    info.append("\n=======================================\n");
                    info.append("电影名: ").append(title).append("\n");
                    info.append("原名: ").append(movie.get("original_title").getAsString()).append("\n");
                    info.append("评分: ").append(movie.get("vote_average").getAsDouble()).append("\n");
                    info.append("上映日期: ").append(movie.get("release_date").getAsString()).append("\n");
                    info.append("简介: ").append(movie.get("overview").getAsString()).append("\n");
                    info.append("=======================================\n");
                    
                    context.write(new Text(info.toString()), NullWritable.get());
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    // ============================================================
    // 主函数 Driver
    // ============================================================
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        if (args.length < 3) {
            System.err.println("Usage: MovieRecommender <mode> <input> <output> [keyword]");
            System.err.println("Mode 1: Genre Recommendation (recommend)");
            System.err.println("Mode 2: Search Movie (search)");
            System.exit(1);
        }

        String mode = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "Movie Task: " + mode);
        job.setJarByClass(MovieRecommender.class);

        if ("recommend".equals(mode)) {
            // 设置体裁推荐任务
            job.setMapperClass(GenreMapper.class);
            job.setReducerClass(GenreReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } else if ("search".equals(mode)) {
            // 设置搜索任务
            if (args.length < 4) {
                System.err.println("Error: Search mode requires a keyword.");
                System.exit(1);
            }
            String keyword = args[3];
            job.getConfiguration().set("search.keyword", keyword); // 传递关键词到 Mapper
            
            job.setMapperClass(SearchMapper.class);
            job.setNumReduceTasks(0); // 搜索不需要 Reduce 阶段
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
        } else {
            System.err.println("Unknown mode: " + mode);
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}