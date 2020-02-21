package ac.cn.saya.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class HDFSClient {

    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        // 1 获取文件系统
        Configuration conf = new Configuration();
        //这里使用HDFS文件系统
        //conf.set("fs.defaultFS","hdfs://masternode:9000");
        conf.set("fs.defaultFS", "hdfs://172.20.1.225:9000");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME", "saya");
        fs = FileSystem.get(conf);
    }

    /**
     * 上传文件到HDFS
     *
     * @throws Exception
     */
    @Test
    public void textAddFileToHDFS() throws Exception {
        Path src = new Path("/Users/liunengkai/project/java/haddop1/src/main/java/ac/cn/saya/topn/topn.txt");//要上传的文件所在路径
        Path dst = new Path("/laboratory/hdfs/topn");//传到hdfs目录下
        fs.copyFromLocalFile(src, dst);//上传文件
    }

    /**
     * 从HDFS中下载文件
     *
     * @throws Exception
     */
    @Test
    public void textDownloadFileToLocal() throws Exception {
        Path src = new Path("/laboratory/hdfs/textCount.txt");//在HDFS中要下载的文件
        Path dst = new Path("/Users/liunengkai/Downloads");//放到何处
        fs.copyToLocalFile(src, dst);//下载文件
    }

    /**
     * 目录操作
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {
        //创建目录
        fs.mkdirs(new Path("/laboratory/mapreduce/wordcount"));

        //删除目录，如果是非空文件夹，参数2必须给值true
        //fs.delete(new Path("/saya"),true);

        //重命名文件或文件夹
        //fs.rename(new Path("/pandora"),new Path("/Saya"));
    }

    /**
     * 使用文件流的方式操作
     *
     * @throws Exception
     */
    @Test
    public void testStream() throws Exception {
        FileInputStream inputStream = new FileInputStream("/Users/liunengkai/Downloads/textCount.txt");//
        FSDataOutputStream outputStream = fs.create(new Path("/laboratory/hdfs/textCount.txt"));//
        IOUtils.copy(inputStream, outputStream);//将inputStream文件中的内容复制到outputStream中
    }

    @After
    public void destory() throws Exception {
        fs.close();
    }

}
