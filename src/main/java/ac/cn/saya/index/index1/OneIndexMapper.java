package ac.cn.saya.index.index1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Title: OneIndexMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 23:04
 * @Description:
 * 第一次处理
 */

public class OneIndexMapper  extends Mapper<LongWritable, Text, Text , IntWritable> {

    String name;
    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名称
        FileSplit split = (FileSplit) context.getInputSplit();

        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取第一行
        String line = value.toString();

        // 2 切割
        String[] fields = line.split(" ");

        for (String word : fields)
        {
            // 3 拼接
            k.set(word + "--" + name);
            v.set(1);
            // 4 写出
            context.write(k, v);
        }
    }
}
