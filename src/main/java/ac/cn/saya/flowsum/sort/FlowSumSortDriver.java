package ac.cn.saya.flowsum.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Title: FlowSumSortDriver
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/13 23:37
 * @Description:
 */

public class FlowSumSortDriver {

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象示例
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://172.20.1.225:9000");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME", "saya");
        Job job = Job.getInstance(configuration);

        // 2 指定本地jar包所在的本地路径
        job.setJarByClass(FlowSumSortDriver.class);

        // 3 本地业务job要使用的mr业务类
        job.setMapperClass(FlowSumSortMapper.class);
        job.setReducerClass(FlowSumSortReducer.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5 指定最终输出的数据kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSortBean.class);


        //设置运行reduceTask的个数
        //job.setNumReduceTasks(5);
        //job.setPartitionerClass(ProvincePartitioner.class);

        FileInputFormat.setInputPaths(job,"/laboratory/hdfs/phone");
        Path outPath = new Path("/laboratory/mapreduce/flow/sort");

		FileSystem fs = FileSystem.get(configuration);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
        FileOutputFormat.setOutputPath(job, outPath);

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
