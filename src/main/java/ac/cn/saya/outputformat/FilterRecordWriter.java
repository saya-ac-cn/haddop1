package ac.cn.saya.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Title: FilterRecordWriter
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/17 22:33
 * @Description:
 */

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    /**
     * 全局的输出流
     */
    FSDataOutputStream sayaOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext taskAttemptContext) {

        // 1 获取文件系统
        FileSystem fs;
        try
        {
            fs = FileSystem.get(taskAttemptContext.getConfiguration());

            // 2 创建输出文件路径
            Path sayaPath = new Path("E:\\linshi\\hadoop\\outputformat\\output\\saya.log");
            Path otherPath = new Path("E:\\linshi\\hadoop\\outputformat\\output\\other.log");

            // 3 创建输出流
            sayaOut = fs.create(sayaPath);
            otherOut = fs.create(otherPath);

        }catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 分类输出
        if(key.toString().contains("saya"))
        {
            sayaOut.write(key.toString().getBytes());
        }
        else
        {
            otherOut.write(key.toString().getBytes());
        }
    }

    /**
     * 关闭资源
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关闭资源
        if (sayaOut != null) {
            sayaOut.close();
        }
        if (otherOut != null) {
            otherOut.close();
        }
    }
}
