package ac.cn.saya.index.process2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Title: TwoIndexMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 23:15
 * @Description:
 */

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    /**
     * 输入的格式：atguigu--a.txt 3
     * 拟输出的格式：k:atguigu v:a.txt 3
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取1行数据
        String line = value.toString();

        // 2 用--切割
        String[] fields = line.split("--");

        k.set(fields[0]);
        v.set(fields[1]);

        context.write(k, v);
    }
}
