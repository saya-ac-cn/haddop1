package ac.cn.saya.wordCount.simple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Title: WordcountDriver
 * @ProjectName haddop1
 * @Description: TODO
 * @Author liunengkai
 * @Date: 2020-02-20 13:45
 * @Description:
 */

public class WordcountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置信息
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.20.1.225:9000");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME", "saya");
        Job job = Job.getInstance(conf);

        // 设置jar加载路径
        job.setJarByClass(WordcountDriver.class);

        // 设置 map 和 Reduce 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 设置 map 输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 Reduce 输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/laboratory/hdfs/textCount.txt"));
        Path outPath = new Path("/laboratory/mapreduce/wordcount/simple");
        // 要输出的目录存在，删除之
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
        // 可能使用命令查看时，会出错
        // 解决参考1：https://www.cnblogs.com/zhi-leaf/p/11424620.html
        // 解决参考2：https://www.cnblogs.com/zlslch/p/6418248.html
    }

}
