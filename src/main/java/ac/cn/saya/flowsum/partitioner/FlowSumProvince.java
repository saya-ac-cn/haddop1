package ac.cn.saya.flowsum.partitioner;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumProvince {

    public static class FlowSumProvinceMapper  extends Mapper<LongWritable,Text,Text,FlowBean> {

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

    public static class FlowSumProvinceReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
        FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlowCount = 0;
            long downFlowCount = 0;
            for(FlowBean bean : values)
            {
                upFlowCount += bean.getUpFlow();
                downFlowCount += bean.getDownFlow();
            }
            v.set(upFlowCount,downFlowCount);
            context.write(key,v);
        }
    }

    public static void main(String[] agrs){
        try
        {
            //通过Job来封装本次mr相关信息
            Configuration conf = new Configuration();
            conf.set("mapreduce.framework.name","local");
            //conf.set("");
            Job job = Job.getInstance(conf);

            //指定本次mr job jar包运行主类
            job.setJarByClass(FlowSumProvince.class);

            //指定本次mr 所用的mapper reducer类
            job.setMapperClass(FlowSumProvinceMapper.class);
            job.setReducerClass(FlowSumProvinceReducer.class);

            //指定本次mr mapper阶段的输出k v 类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);

            //指定本次mr mapper最终输出的 k v 类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            //设置运行reduceTask的个数
            job.setNumReduceTasks(6);

            //自定义分区的组件
            job.setPartitionerClass(ProvincePartitioner.class);

            //指定本次mr 输入的数据路径 和最终输出存放在什么位置
            FileInputFormat.setInputPaths(job,"E:\\linshi\\hadoop\\flowSum\\input");
            FileOutputFormat.setOutputPath(job,new Path("E:\\linshi\\hadoop\\flowSum\\output"));

            //提交程序，并且监控打印程序情况
            boolean b = job.waitForCompletion(true);
            System.exit(b?0:1);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
