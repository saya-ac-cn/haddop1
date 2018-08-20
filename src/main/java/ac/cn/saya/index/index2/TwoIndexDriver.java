package ac.cn.saya.index.index2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Title: TwoIndexDriver
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 23:24
 * @Description:
 */

public class TwoIndexDriver {

    public static void main(String[] args) throws Exception {
        args = new String[] { "E:\\linshi\\hadoop\\index\\index2\\input", "E:\\linshi\\hadoop\\index\\index2\\output" };

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(TwoIndexDriver.class);

        job.setMapperClass(TwoIndexMapper.class);
        job.setReducerClass(TwoIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }


}
