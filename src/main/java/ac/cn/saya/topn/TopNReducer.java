package ac.cn.saya.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @Title: TopNReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author liunengkai
 * @Date: 2020-02-21 20:42
 * @Description:
 */

public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    //定义一个TreeMap作为存储数据的天然容器（天然按key排序）
    private TreeMap<FlowBean, Text> flowMap = new TreeMap<>();


    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            FlowBean bean = new FlowBean();
            bean.set(key.getDownFlow(), key.getUpFlow());

            // 向TreeMap添加数据
            flowMap.put(bean, new Text(value));

            // 限制TreeMap的数据量，超过10条就删除掉最小的一条数据
            if (flowMap.size() > 10) {
                flowMap.remove(flowMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 3 遍历集合，输出数据
        Iterator<FlowBean> it = flowMap.keySet().iterator();

        while (it.hasNext()) {

            FlowBean v = it.next();

            context.write(new Text(flowMap.get(v)), v);
        }

    }
}
