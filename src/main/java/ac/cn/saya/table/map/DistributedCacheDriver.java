package ac.cn.saya.table.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @Title: DistributedCacheDriver
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/18 22:15
 * @Description:
 */

public class DistributedCacheDriver {

    public static void main(String[] args) throws Exception{
        // 1 获取 job 信息
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://172.20.1.225:9000");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME", "saya");
        Job job = Job.getInstance(configuration);

        // 2 设置加载 jar 包路径
        job.setJarByClass(DistributedCacheDriver.class);

        // 3 关联 map
        job.setMapperClass(DistributedCacheMapper.class);

        // 4 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/laboratory/hdfs/table/order.txt"));
        Path outPath = new Path("/laboratory/mapreduce/table/join");
        // 要输出的目录存在，删除之
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 6 加载缓存数据
        job.addCacheFile(new URI("/laboratory/hdfs/table/pd.txt"));

        // 7 map 端 join 的逻辑不需要 reduce 阶段，设置 reducetask 数量为 0
        job.setNumReduceTasks(0);

        // 8 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
