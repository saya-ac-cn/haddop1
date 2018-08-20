package ac.cn.saya.etl2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Title: LogMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 22:46
 * @Description:
 */

public class LogMapper  extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // 1 获取1行
        String line = value.toString();

        // 2 解析日志是否合法
        LogBean bean = pressLog(line);

        if(!bean.isValid())
        {
            return;
        }

        k.set(bean.toString());

        // 3 输出
        context.write(k, NullWritable.get());

    }

    /**
     * 逐行解析日志
     * @param line
     * @return
     */
    private LogBean pressLog(String line)
    {
        LogBean logBean = new LogBean();

        // 1 截取
        String [] fields = line.split(" ");

        if (fields.length > 11)
        {
            // 2 封装数据
            logBean.setRemoteAddr(fields[0]);
            logBean.setRemoteUser(fields[1]);
            logBean.setTimeLocal(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBodyBytesSent(fields[9]);
            logBean.setHttpReferer(fields[10]);
            if (fields.length > 12)
            {
                logBean.setHttpUserAgent(fields[11]+" "+ fields[12]);
            }
            else
            {
                logBean.setHttpUserAgent(fields[11]);
            }
            // 大于 400，HTTP 错误
            if(Integer.parseInt(logBean.getStatus()) >= 400)
            {
                logBean.setValid(false);
            }
        }
        else
        {
            logBean.setValid(false);
        }
        return logBean;
    }

}
