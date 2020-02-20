package ac.cn.saya.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Title: OrderPartitioner
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/15 23:20
 * @Description:
 */

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {


    @Override
    public int getPartition(OrderBean key, NullWritable value, int i) {
        return (key.getOrderId() & Integer.MAX_VALUE) % i;
    }
}
