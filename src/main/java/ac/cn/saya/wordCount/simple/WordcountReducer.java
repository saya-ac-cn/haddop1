package ac.cn.saya.wordCount.simple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title: WordcountReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author liunengkai
 * @Date: 2020-02-20 13:42
 * @Description:
 */

public class WordcountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 累计求和
        int sum = 0;
        for (IntWritable count:values){
            sum += count.get();
        }
        // 输出
        context.write(key,new IntWritable(sum));
    }
}
