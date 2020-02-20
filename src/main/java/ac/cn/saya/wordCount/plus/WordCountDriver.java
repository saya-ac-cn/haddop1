package ac.cn.saya.wordCount.plus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    /**
     * hadoop jar wordcount.jar ac.cn.saya.wordCount.WordCountDriver /wordCount/input/ /wordCount/output
     * 这个类就是mr程序运行时候的主类，本类中组装了一些程序运行时所需要的信息
     * 比如：使用的那个Mapper类，哪个Reducer类 输入和输出数据在哪
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        //通过Job来封装本次mr相关信息
        Configuration  conf = new Configuration();
        //conf.set("mapreduce.framework.name","local");
        //conf.set("");
        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(WordCountDriver.class);

        //指定本次mr 所用的mapper reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定本次mapper阶段的输出k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定本次mr 最终输出的 k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 对每一个 maptask 的输出局部汇总（Combiner
        job.setCombinerClass(WordCountCombiner.class);
        // 等同于原来的reducer
        //job.setCombinerClass(WordCountReducer.class);

        // 设置InputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);// 4M
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);// 2M

        //指定本次mr 输入的数据路径 和最终输出存放在什么位置
        //FileInputFormat.setInputPaths(job,"E:\\linshi\\hadoop\\wordCount\\input");
        //FileOutputFormat.setOutputPath(job,new Path("E:\\linshi\\hadoop\\wordCount\\output"));
        //打包后可能会出现错误：
        // Exception in thread "main" java.lang.SecurityException: Invalid signature file digest for Manifest main attributes
        // 解决方法zip -d spark_scala_demo.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF

        // 指定job的输入、输出文件所在目录
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //Path outPath = new Path(args[1]);

        FileInputFormat.setInputPaths(job,"E:\\linshi\\hadoop\\wordCount\\input");
        Path outPath = new Path("E:\\linshi\\hadoop\\wordCount\\output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //提交程序，并且监控打印程序情况
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }


}
