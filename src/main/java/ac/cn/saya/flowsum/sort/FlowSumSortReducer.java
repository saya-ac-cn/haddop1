package ac.cn.saya.flowsum.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title: FlowSumSortReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/13 23:32
 * @Description:
 */

public class FlowSumSortReducer extends Reducer<FlowSortBean,Text,Text,FlowSortBean> {


    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(),key);
    }
}
