package ac.cn.saya.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @Title: TopNMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author liunengkai
 * @Date: 2020-02-21 20:29
 * @Description:
 */

public class TopNMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    //定义一个TreeMap作为存储数据的天然容器（天然按key排序）
    private TreeMap<FlowBean,Text> flowMap = new TreeMap<>();
    private FlowBean k;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        k = new FlowBean();
        Text v = new Text();

        // 获取一行
        String line = value.toString();

        // 切割
        String[] fields = line.split("\t");

        // 数据封装
        String phone = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);
        k.setDownFlow(downFlow);
        k.setUpFlow(upFlow);
        k.setSumFlow(sumFlow);

        v.set(phone);

        // 向TreeMap添加数据
        flowMap.put(k,v);

        // 限制TreeMap的数据量，超过10条就删除掉最小的一条数据
        if (flowMap.size() > 10){
            flowMap.remove(flowMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 遍历TreeMap集合，输出数据
        Iterator<FlowBean> iterator = flowMap.keySet().iterator();
        while (iterator.hasNext()){
            FlowBean k = iterator.next();
            context.write(k,flowMap.get(k));
        }
    }
}
