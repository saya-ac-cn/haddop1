package ac.cn.saya.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class FlowSumDriver {
    private static final Logger LOGGER = Logger .getLogger(FlowSumDriver.class);

    public static void main(String[] args){
        try
        {
            //通过Job来封装本次mr相关信息
            Configuration conf = new Configuration();
            //conf.set("mapreduce.framework.name","local");
            //conf.set("");
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

            //指定本次mr 输入的数据路径 和最终输出存放在什么位置
            FileInputFormat.setInputPaths(job,"E:\\linshi\\hadoop\\flowSum\\input");
            FileOutputFormat.setOutputPath(job,new Path("E:\\linshi\\hadoop\\flowSum\\output"));
            //FileInputFormat.setInputPaths(job,new Path(args[0]));
            //FileOutputFormat.setOutputPath(job,new Path(args[1]));

            //提交程序，并且监控打印程序情况
            boolean b = job.waitForCompletion(true);
            System.exit(b?0:1);
        }catch (Exception e)
        {
            e.printStackTrace();
            LOGGER.error(e);
        }
    }

}
