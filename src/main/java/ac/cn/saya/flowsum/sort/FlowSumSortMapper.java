package ac.cn.saya.flowsum.sort;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Title: FlowSumSortMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/13 23:20
 * @Description:
 */

public class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowSortBean, Text> {

    FlowSortBean k = new FlowSortBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割字段
        String[] fields = line.split("\t");

        // 3 封装对象
        // 取出手机号码
        String phoneNum = fields[0];
        // 取出上行流量、下行流量
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        // 封装对象
        k.set(upFlow, downFlow);
        v.set(phoneNum);

        // 输出
        context.write(k, v);

    }
}
