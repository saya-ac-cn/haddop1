package ac.cn.saya.inputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @Title: MyRecordReader
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/16 22:57
 * @Description:
 */

public class MyRecordReader extends RecordReader<NullWritable,BytesWritable> {

    private Configuration configuration;
    private FileSplit split;
    private boolean processed = false;
    private BytesWritable value = new BytesWritable();

    /**
     * 初始化
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed)
        {
            // 1 定义缓冲区
            byte[] contents = new byte[(int) split.getLength()];// 设置缓冲区的大小
            FileSystem fs = null;
            FSDataInputStream fin = null;
            try
            {
                // 2 获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);

                // 3 读取数据
                fin = fs.open(path);

                // 4 读取文件内容
                IOUtils.readFully(fin,contents,0,contents.length);

                // 5 输出文件内容
                value.set(contents,0,contents.length);

            }catch (Exception e)
            {
                e.printStackTrace();
            }finally {
                IOUtils.closeStream(fin);
            }
            processed = true;
            return processed;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
