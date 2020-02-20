package ac.cn.saya.flowsum.simple;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowBean,Text,FlowBean> {
    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlowCount = 0;
        long downFlowCount = 0;

        // 1 遍历所用的备案，将其中的上行流量、下行流量分别累加
        for(FlowBean bean : values)
        {
            upFlowCount += bean.getUpFlow();
            downFlowCount += bean.getDownFlow();
        }

        // 2 封装对象 并写入
        v.set(upFlowCount,downFlowCount);
        context.write(key,v);
    }
}
