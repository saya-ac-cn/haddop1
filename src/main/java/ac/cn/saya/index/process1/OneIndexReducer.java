package ac.cn.saya.index.process1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title: OneIndexReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 23:10
 * @Description:
 */

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 写出后的格式：atguigu--a.txt 2
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            // 1 累加求和
            count += value.get();
        }
        // 2 写出
        context.write(key, new IntWritable(count));
    }
}
