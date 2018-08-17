package ac.cn.saya.tablejoin.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Title: TableDriver
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/18 00:03
 * @Description:
 */

public class TableDriver {

    public static void main(String[] args) throws Exception {
        args = new String[] { "E:\\linshi\\hadoop\\tablejoin\\input", "E:\\linshi\\hadoop\\tablejoin\\output" };
        // 1 获取配置信息，或者 job 对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的 jar 包所在的本地路径
        job.setJarByClass(TableDriver.class);

        // 3 指定本业务 job 要使用的 mapper/Reducer 业务类
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        // 4 指定 mapper 输出数据的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 5 指定最终输出的数据的 kv 类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 指定 job 的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 7 将 job 中配置的相关参数，以及 job 所用的 java 类所在的 jar 包， 提交给yarn 去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
