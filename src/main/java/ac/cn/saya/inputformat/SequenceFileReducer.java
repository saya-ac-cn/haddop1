package ac.cn.saya.inputformat;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Title: SequenceFileReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/16 23:13
 * @Description:
 */

public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text,
        BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
