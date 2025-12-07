import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class MovieRecommender {

    // 全局使用的 Gson 解析器
    private static final Gson gson = new Gson();

    // ============================================================
    // 模式 1: 普通体裁推荐 (Genre Recommendation)
    // ============================================================
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JsonObject movie = gson.fromJson(value.toString(), JsonObject.class);
                String title = movie.get("title").getAsString();
                double score = movie.has("vote_average") ? movie.get("vote_average").getAsDouble() : 0.0;
                String releaseDate = movie.has("release_date") ? movie.get("release_date").getAsString() : "Unknown";
                
                // 输出格式: 评分::电影名::日期
                String movieInfo = score + "::" + title + "::" + releaseDate;

                if (movie.has("genres")) {
                    for (JsonElement g : movie.getAsJsonArray("genres")) {
                        context.write(new Text(g.getAsString()), new Text(movieInfo));
                    }
                }
            } catch (Exception e) { /* 忽略坏数据 */ }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movies = new ArrayList<>();
            for (Text val : values) movies.add(val.toString());
            
            // 简单字符串排序 (降序)
            Collections.sort(movies, Collections.reverseOrder());

            StringBuilder sb = new StringBuilder();
            int count = 0;
            for (String m : movies) {
                if (count++ >= 10) break;
                String[] parts = m.split("::");
                if (parts.length >= 2) {
                    if (sb.length() > 0) sb.append(" | ");
                    sb.append(parts[1]).append("(").append(parts[0]).append(")");
                }
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    // ============================================================
    // 模式 2: 电影名字搜索 (Search)
    // ============================================================
    public static class SearchMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private String keyword;
        @Override
        protected void setup(Context context) {
            keyword = context.getConfiguration().get("search.keyword", "").toLowerCase();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JsonObject movie = gson.fromJson(value.toString(), JsonObject.class);
                String title = movie.get("title").getAsString();
                if (title.toLowerCase().contains(keyword)) {
                    String out = String.format("\n[Found] Title: %s | Score: %s | Date: %s\nOverview: %s\n", 
                        title, movie.get("vote_average"), movie.get("release_date"), movie.get("overview"));
                    context.write(new Text(out), NullWritable.get());
                }
            } catch (Exception e) { }
        }
    }

    // ============================================================
    // 模式 3: 多维度组合推荐 (Multi-Dimension Recommendation)
    // Key 格式: "体裁::国家::年份"
    // ============================================================
    public static class MultiDimMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JsonObject movie = gson.fromJson(value.toString(), JsonObject.class);
                
                String title = movie.get("title").getAsString();
                double score = movie.has("vote_average") ? movie.get("vote_average").getAsDouble() : 0.0;
                
                // 提取年份
                String year = "Unknown";
                if (movie.has("release_date")) {
                    String date = movie.get("release_date").getAsString();
                    if (date.length() >= 4) year = date.substring(0, 4);
                }

                // Value: 评分::电影名
                String movieInfo = score + "::" + title;

                // 准备数组
                JsonArray genres = movie.has("genres") ? movie.getAsJsonArray("genres") : new JsonArray();
                JsonArray countries = movie.has("production_countries") ? movie.getAsJsonArray("production_countries") : new JsonArray();

                // 处理无国家的情况
                List<String> countryList = new ArrayList<>();
                if (countries.size() == 0) {
                    countryList.add("Other");
                } else {
                    for (JsonElement c : countries) countryList.add(c.getAsString());
                }

                // 双重循环：笛卡尔积
                for (JsonElement g : genres) {
                    String genreName = g.getAsString();
                    for (String countryName : countryList) {
                        // Key: 剧情::United States of America::2025
                        String compositeKey = genreName + "::" + countryName + "::" + year;
                        context.write(new Text(compositeKey), new Text(movieInfo));
                    }
                }
            } catch (Exception e) { }
        }
    }

    public static class MultiDimReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movies = new ArrayList<>();
            for (Text val : values) {
                movies.add(val.toString());
            }

            // 自定义排序：解析评分 Double 进行比较，确保 10.0 排在 9.0 前面
            // Value 格式: "7.922::Frankenstein"
            Collections.sort(movies, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    try {
                        double s1 = Double.parseDouble(o1.split("::")[0]);
                        double s2 = Double.parseDouble(o2.split("::")[0]);
                        return Double.compare(s2, s1); // s2 - s1 为降序
                    } catch (Exception e) {
                        return 0;
                    }
                }
            });

            StringBuilder result = new StringBuilder();
            int count = 0;
            for (String m : movies) {
                if (count > 0) result.append(" | ");
                String[] parts = m.split("::");
                if (parts.length >= 2) {
                    // 输出: 电影名(评分)
                    result.append(parts[1]).append("(").append(parts[0]).append(")");
                }
                count++;
                if (count >= 10) break; // Top 10
            }
            context.write(key, new Text(result.toString()));
        }
    }

    // ============================================================
    // 主程序入口
    // ============================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        if (args.length < 3) {
            System.err.println("Usage: MovieRecommender <mode> <input> <output> [keyword]");
            System.err.println("Modes: recommend | search | multidim");
            System.exit(1);
        }

        String mode = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "Movie Task: " + mode);
        job.setJarByClass(MovieRecommender.class);

        // 设置通用路径
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 根据模式配置 Mapper/Reducer
        if ("recommend".equals(mode)) {
            job.setMapperClass(GenreMapper.class);
            job.setReducerClass(GenreReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } 
        else if ("search".equals(mode)) {
            if (args.length < 4) {
                System.err.println("Error: Search needs a keyword.");
                System.exit(1);
            }
            job.getConfiguration().set("search.keyword", args[3]);
            job.setMapperClass(SearchMapper.class);
            job.setNumReduceTasks(0); // 没有 Reduce
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
        } 
        else if ("multidim".equals(mode)) {
            // 新增的模式
            job.setMapperClass(MultiDimMapper.class);
            job.setReducerClass(MultiDimReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } 
        else {
            System.err.println("Unknown mode: " + mode);
            System.exit(1);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}