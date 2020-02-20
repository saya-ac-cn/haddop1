package ac.cn.saya.flowsum.partitioner;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 将统计结果按照手机归属地不同省份输出到不同文件
 */
public class FlowSumProvinceDriver {

    public static class FlowSumProvinceMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        Text k = new Text();
        FlowBean v = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            //LOGGER.debug("读数组："+fields[1]);
            String phoneNum = fields[1];
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);
            k.set(phoneNum);
            v.set(upFlow, downFlow);
            context.write(k, v);
        }
    }

    public static class FlowSumProvinceReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        FlowBean v = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlowCount = 0;
            long downFlowCount = 0;
            for (FlowBean bean : values) {
                upFlowCount += bean.getUpFlow();
                downFlowCount += bean.getDownFlow();
            }
            v.set(upFlowCount, downFlowCount);
            context.write(key, v);
        }
    }

    public static void main(String[] args) {
        try {
            //通过Job来封装本次mr相关信息
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://172.20.1.225:9000");
            //设置客户端身份
            System.setProperty("HADOOP_USER_NAME", "saya");
            Job job = Job.getInstance(conf);

            //指定本次mr job jar包运行主类
            job.setJarByClass(FlowSumProvinceDriver.class);

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

            // 设置输入和输出路径
            FileInputFormat.setInputPaths(job, new Path("/laboratory/hdfs/flow.txt"));
            Path outPath = new Path("/laboratory/mapreduce/flow/partitioner");
            // 要输出的目录存在，删除之
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);


            //提交程序，并且监控打印程序情况
            boolean b = job.waitForCompletion(true);
            System.exit(b ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
