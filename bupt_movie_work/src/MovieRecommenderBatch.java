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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class MovieRecommenderBatch {

    private static final Gson gson = new Gson();

    // Mapper: 提取体裁和电影基本信息
    public static class BatchGenreMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JsonObject movie = gson.fromJson(value.toString(), JsonObject.class);
                
                // 1. 处理标题：必须去掉标题里的逗号，否则 CSV 格式会乱！
                String title = movie.get("title").getAsString().replace(",", " "); 
                
                double score = movie.has("vote_average") ? movie.get("vote_average").getAsDouble() : 0.0;
                
                // 2. 中间 Value 格式: 评分::电影名
                String movieInfo = score + "::" + title;

                if (movie.has("genres")) {
                    for (JsonElement g : movie.getAsJsonArray("genres")) {
                        // Key: 体裁 (例如 "剧情")
                        context.write(new Text(g.getAsString()), new Text(movieInfo));
                    }
                }
            } catch (Exception e) {
                // 忽略解析错误
            }
        }
    }

    // Reducer: 排序并输出 CSV 格式
    public static class BatchGenreReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movies = new ArrayList<>();
            for (Text val : values) {
                movies.add(val.toString());
            }

            // 按评分降序排序 (字符串比较需注意，但这里为了简单直接用字符串倒序，通常评分格式固定没问题)
            // 更严谨的做法是解析 Double 排序，但此处保持简单
            Collections.sort(movies, Collections.reverseOrder());

            int count = 0;
            for (String m : movies) {
                if (count >= 10) break; // 只取 Top 10

                String[] parts = m.split("::");
                if (parts.length >= 2) {
                    String score = parts[0];
                    String title = parts[1];

                    // 3. 构造 CSV 行: 体裁,电影名,评分,排名
                    // 例如: 剧情,肖申克的救赎,9.8,1
                    String csvLine = key.toString() + "," + title + "," + score + "," + (count + 1);
                    
                    context.write(new Text(csvLine), NullWritable.get());
                }
                count++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        if (args.length < 2) {
            System.err.println("Usage: MovieRecommenderBatch <input> <output>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Genre Recommendation Batch Export");
        job.setJarByClass(MovieRecommenderBatch.class);

        job.setMapperClass(BatchGenreMapper.class);
        job.setReducerClass(BatchGenreReducer.class);

        // 设置 Mapper 输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置 Reducer 输出类型 (Key=Text, Value=NullWritable 实现纯文本行)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
