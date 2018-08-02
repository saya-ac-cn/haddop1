package ac.cn.saya;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    /**
     * 这个类就是mr程序运行时候的主类，本类中组装了一些程序运行时所需要的信息
     * 比如：使用的那个Mapper类，哪个Reducer类 输入和输出数据在哪
     * @param agrs
     * @throws Exception
     */
    public static void main(String[] agrs) throws Exception{
        //通过Job来封装本次mr相关信息
        Configuration  conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        //conf.set("");
        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(WordCountDriver.class);

        //指定本次mr 所用的mapper reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定本次mr mapper阶段的输出k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定本次mr 最终输出的 k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定本次mr 输入的数据路径 和最终输出存放在什么位置
        FileInputFormat.setInputPaths(job,"E:\\linshi\\hadoop\\wordCount\\input");
        FileOutputFormat.setOutputPath(job,new Path("E:\\linshi\\hadoop\\wordCount\\output"));

        //提交程序，并且监控打印程序情况
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }


}
