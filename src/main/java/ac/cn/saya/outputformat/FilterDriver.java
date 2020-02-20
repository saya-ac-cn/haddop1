package ac.cn.saya.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Title: FilterDriver
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/17 23:19
 * @Description:
 */

public class FilterDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.20.1.225:9000");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME", "saya");
        Job job = Job.getInstance(conf);

        job.setJarByClass(FilterDriver.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 要将自定义的输出格式组件设置到 job 中
        job.setOutputFormatClass(FilterOutputFormat.class);

        // 虽然我们自定义了 outputformat，但是因为我们的 outputformat 继承自fileoutputformat
        // 而 fileoutputformat 要输出一个_SUCCESS 文件，所以，在这还得指定一个输出目录
        FileInputFormat.setInputPaths(job, new Path("/laboratory/hdfs/log.txt"));
        Path outPath = new Path("/laboratory/mapreduce/output/result");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
