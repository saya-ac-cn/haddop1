package ac.cn.saya.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 使用API操作HDFS
 */
public class HDFSUseApi {


    FileSystem fs = null;

    @Before
    //初始化文件系统
    public void init() throws Exception
    {
        // 1 获取文件系统
        Configuration conf = new Configuration();
        //这里使用HDFS文件系统
        conf.set("fs.defaultFS","hdfs://172.20.1.225:9000");
        //设置副本数
        conf.set("dfs.replication","3");
        //设置客户端身份
        System.setProperty("HADOOP_USER_NAME","saya");
        //获取文件系统
        fs = FileSystem.get(conf);
        //打印文件系统
        System.out.println(fs.toString());
    }

    /**
     * 上传本地文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception
    {
        Path src = new Path("/Users/liunengkai/project/java/haddop1/src/main/java/ac/cn/saya/flowsum/flow.txt");//要上传的文件的位置
        Path dst = new Path("/laboratory/hdfs");//放在hdfs的何处
        //上传
        fs.copyFromLocalFile(src,dst);
    }

    /**
     * 下载文件
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception
    {
        Path dst = new Path("/serch.html");//下载源
        Path src = new Path("C:/Users/Saya/Desktop/serch.html");//下载后存放的位置
        //下载文件
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        fs.copyToLocalFile(false,dst,src);
    }

    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void madirs() throws Exception
    {
        Path src = new Path("/laboratory/hdfs/topn");//要创建的目录
        //创建目录
        fs.mkdirs(src);
        //关闭
        fs.close();
    }

    /**
     * 删除文件或目录
     * @throws Exception
     */
    @Test
    public void delete() throws Exception
    {
        Path src = new Path("/laboratory/hdfs/table");//要删除的文件或目录
        //删除
        fs.delete(src,true);//第二个参数表示是否递归删除
    }

    /**
     * 重命名文件或目录
     * @throws Exception
     */
    @Test
    public void rename() throws Exception
    {
        Path oldName = new Path("/serch.html");//原来的额文件、目录名字
        Path newName = new Path("/test.html");//新的文件、目录名字
        //执行重命名
        fs.rename(oldName,newName);
    }

    @After
    public void close() throws Exception{
        //关闭
        fs.close();
    }

}
