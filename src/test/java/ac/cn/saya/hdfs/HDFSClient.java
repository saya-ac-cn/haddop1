package ac.cn.saya.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.io.FileInputStream;
import java.io.IOException;

public class HDFSClient {

    FileSystem fs = null;

    @Before
    public void init() throws Exception
    {
        // 1 获取文件系统
        Configuration conf = new Configuration();
        //这里使用HDFS文件系统
        conf.set("fs.defaultFS","hdfs://masternode:9000");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME","saya");

        fs = FileSystem.get(conf);
    }

    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void textAddFileToHDFS() throws Exception
    {
        Path src =  new Path("C:/Users/Saya/Desktop/data.txt");//要上传的文件所在路径
        Path dst = new Path("/");//传到根目录下
        fs.copyFromLocalFile(src,dst);//上传文件
        fs.close();//及时关闭
    }

    /**
     * 从HDFS中下载文件
     * @throws Exception
     */
    @Test
    public void textDownloadFileToLocal() throws Exception
    {
        Path src =  new Path("/data.txt");//在HDFS中要下载的文件
        Path dst = new Path("e://");//放到何处
        fs.copyToLocalFile(src,dst);//下载文件

        fs.close();//及时关闭
    }

    /**
     * 目录操作
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException,IOException{
        //创建目录
        fs.mkdirs(new Path("/YuanHuiTing"));
        fs.mkdirs(new Path("/pandora"));

        //删除目录，如果是非空文件夹，参数2必须给值true
        fs.delete(new Path("/tingJava"),true);

        //重命名文件或文件夹
        fs.rename(new Path("/pandora"),new Path("/Saya"));

        fs.close();//及时关闭
    }

    /**
     * 使用文件流的方式操作
     * @throws Exception
     */
    @Test
    public void testStream() throws Exception
    {
        FSDataOutputStream outputStream = fs.create(new Path("/1.txt"));//

        FileInputStream inputStream = new FileInputStream("C:\\Users\\Saya\\Desktop\\临时.txt");//

        IOUtils.copy(inputStream,outputStream);

        fs.close();
    }

}
