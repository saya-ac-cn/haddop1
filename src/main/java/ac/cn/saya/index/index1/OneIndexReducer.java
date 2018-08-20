package ac.cn.saya.index.index1;

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

public class OneIndexReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // 1 累加求和
        for(IntWritable value : values)
        {
            count += value.get();
        }
        // 2 写出
        context.write(key, new IntWritable(count));
    }
}
