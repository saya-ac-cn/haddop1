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

        // 6 指定job的输入、输出文件所在目录
        //FileInputFormat.setInputPaths(job,args[0]);
        //Path outPath = new Path(args[1]);

        FileInputFormat.setInputPaths(job,"E:\\linshi\\hadoop\\flowSumSort\\input");
        Path outPath = new Path("E:\\linshi\\hadoop\\flowSumSort\\ouput");

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
