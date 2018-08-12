package ac.cn.saya.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;


/**
 * 使用io流操作hdfs
 */
public class HDFSUseIOStream {

    Configuration conf = null;
    FileSystem fs = null;

    @Before
    //初始化文件系统
    public void init() throws Exception
    {
        // 1 获取文件系统
        this.conf = new Configuration();
        //这里使用HDFS文件系统
        this.conf.set("fs.defaultFS","hdfs://masternode:9000");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME","saya");
        //获取文件系统
        this.fs = FileSystem.get(this.conf);
        //打印文件系统
        System.out.println(this.fs.toString());
    }

    /**
     * 文件上传
     * @throws IOException
     */
    @Test
    public void putFileToHDFS() throws IOException {
        // 获取输入流
        FileInputStream fis = new FileInputStream(new File("C:/Users/Saya/Desktop/test.html"));

        // 获取输出流
        FSDataOutputStream fos = this.fs.create(new Path("/p/test.html"));
        try
        {
            // 执行上传
            IOUtils.copyBytes(fis,fos,this.conf);
        }catch (Exception e)
        {
            System.err.println(e.getMessage());
        }finally {
            // 执行关闭
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }

    /**
     * 下载文件
     * @throws IOException
     */
    @Test
    public  void getFileFromHDFS() throws IOException {
        //获取输入流
        FSDataInputStream fis = this.fs.open(new Path("/p/test.html"));

        //创建输出流
        FileOutputStream fos = new FileOutputStream(new File("C:/Users/Saya/Desktop/test1.html"));
        try
        {
            //执行复制
            IOUtils.copyBytes(fis,fos,this.conf);
        }catch (Exception e)
        {
            System.err.println(e.getMessage());
        }finally {
            //关闭资源
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }

    /**
     * 读取第一块
     * @throws IOException
     */
    @Test
    public  void getFileFromHDFSSeek1() throws IOException {
        //获取输入流
        FSDataInputStream fis = this.fs.open(new Path("/p/test.html"));

        //创建输出流
        FileOutputStream fos = new FileOutputStream(new File("C:/Users/Saya/Desktop/test1.html"));
        try
        {
            //流对接 只取128M
            byte[] buf = new byte[1024];
            //1024*1024*128
            for (int i = 0;i<1024*128;i++)
            {
                fis.read(buf);
                fos.write(buf);
            }
        }catch (Exception e)
        {
            System.err.println(e.getMessage());
        }finally {
            //关闭资源
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        }
    }



}
