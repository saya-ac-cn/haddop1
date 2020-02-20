package ac.cn.saya.flowsum.simple;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable,Text,Text, FlowBean> {
    private static final Logger LOGGER = Logger .getLogger(FlowSumMapper.class);

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 将一行内容转成string
        String line = value.toString();

        // 2 切分字段
        String [] fields = line.split("\t");

        // 3 取出手机号
        String phoneNum = fields[1];

        // 4 取出上行流量、下行流量
        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);

        // 5 写数据
        k.set(phoneNum);
        v.set(upFlow,downFlow);
        context.write(k,v);
    }
}
