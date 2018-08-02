package ac.cn.saya.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    private static final Logger LOGGER = Logger .getLogger(FlowSumMapper.class);
    Text k = new Text();
    FlowBean v = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] fields = line.split("\t");
        //LOGGER.debug("读数组："+fields[1]);
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);
        k.set(phoneNum);
        v.set(upFlow,downFlow);
        context.write(k,v);
    }
}
