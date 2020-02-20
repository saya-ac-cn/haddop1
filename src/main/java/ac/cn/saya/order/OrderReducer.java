package ac.cn.saya.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title: OrderReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/15 23:28
 * @Description:
 */

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        // 每次只打印第一个key
        context.write(key, NullWritable.get());
    }


}
