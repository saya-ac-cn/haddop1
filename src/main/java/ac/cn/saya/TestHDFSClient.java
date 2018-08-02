package ac.cn.saya;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHDFSClient {

    public static void main(String[] agrs) throws Exception
    {
        Configuration conf = new Configuration();
        //这里使用HDFS文件系统
        conf.set("fs.defaultFS","hdfs://localhost:9000");

        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME","Saya");

        FileSystem fs = FileSystem.get(conf);

        //使用java创建一个目录
        //fs.create(new Path("/tingJava"));

        fs.copyToLocalFile(new Path("/saya.log"),new Path("e://"));

        fs.close();
    }

}
