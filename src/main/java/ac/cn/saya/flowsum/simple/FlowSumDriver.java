package ac.cn.saya.flowsum.simple;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * 需求 1：统计手机号耗费的总上行流量、下行流量、总流量（序列化）
 */
public class FlowSumDriver {
    private static final Logger LOGGER = Logger.getLogger(FlowSumDriver.class);

    public static void main(String[] args) {
        try {
            //通过Job来封装本次mr相关信息
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://172.20.1.225:9000");
            //设置客户端身份
            System.setProperty("HADOOP_USER_NAME", "saya");
            Job job = Job.getInstance(conf);

            //指定本次mr job jar包运行主类
            job.setJarByClass(FlowSumDriver.class);

            //指定本次mr 所用的mapper reducer类
            job.setMapperClass(FlowSumMapper.class);
            job.setReducerClass(FlowSumReducer.class);

            //指定本次mr mapper阶段的输出k v 类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);

            //指定本次mr mapper最终输出的 k v 类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            // 设置输入和输出路径
            FileInputFormat.setInputPaths(job, new Path("/laboratory/hdfs/flow.txt"));
            Path outPath = new Path("/laboratory/mapreduce/flow/simple");
            // 要输出的目录存在，删除之
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            //提交程序，并且监控打印程序情况
            boolean b = job.waitForCompletion(true);
            System.exit(b ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e);
        }
    }

}
