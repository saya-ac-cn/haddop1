package ac.cn.saya.tablejoin.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Title: TableMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/17 23:39
 * @Description:
 */

public class TableMapper  extends Mapper<LongWritable, Text, Text, TableBean> {

    TableBean bean = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取输入文件类型
        FileSplit split = (FileSplit)context.getInputSplit();
        String name = split.getPath().getName();// 获取文件名

        // 2 获取输入数据
        String line = value.toString();

        // 3切割
        String[] fields = line.split("\t");

        // 4 不同的文件分别处理
        if(name.startsWith("order"))
        {
            // 处理订单表数据

            // 封装bean
            bean.setOrderId(fields[0]);
            bean.setpId(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setPname("");
            bean.setFlag("0");

            k.set(fields[1]);
        }
        else
        {
            // 产品表处理

            // 封装bean
            bean.setpId(fields[0]);
            bean.setPname(fields[1]);
            bean.setAmount(0);
            bean.setOrderId("");
            bean.setFlag("1");

            k.set(fields[0]);
        }
        // 4 写出
        context.write(k, bean);
    }
}
