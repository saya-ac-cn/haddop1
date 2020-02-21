package ac.cn.saya.table.map;

import ac.cn.saya.tools.StringGetEnCodeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: DistributedCacheMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/18 22:09
 * @Description:
 */

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    // 贮存商品明细的hashMap:key->商品id，value->商品名
    Map<String, String> pdMap = new HashMap<>();

    Text k = new Text();

    /**
     * 将商品预先缓存到内存中，供map使用
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0].getPath().toString());
        FileSystem fs = FileSystem.get(context.getConfiguration());

        // 1 获取缓存的文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path),"UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 2 切割
            String[] fields = line.split("\t");

            // 3 缓存数据到集合
            pdMap.put(fields[0].trim(), fields[1]);
            System.out.println(fields[0].trim()+":编码"+ StringGetEnCodeUtil.getEncoding(fields[0].trim()));

        }
        reader.close();

//        URI[] cacheFiles = context.getCacheFiles();
//        String path = cacheFiles[0].getPath().toString();
//
//        // 1 获取缓存的文件
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
//
//        String line;
//        while(StringUtils.isNotEmpty(line = reader.readLine())){
//
//            // 2 切割
//            String[] fields = line.split("\t");
//
//            // 3 缓存数据到集合
//            pdMap.put(fields[0], fields[1]);
//        }
//
//        // 4 关流
//        reader.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 截取
        String[] fields = line.split("\t");

        // 3 获取产品 id
        String pId = fields[1].trim();

        // 4 获取商品名称
        String pdName = this.pdMap.get(pId);

        System.out.println(fields[1]+"->编码"+ StringGetEnCodeUtil.getEncoding(fields[1]));
        // 6 写出
        context.write(k, NullWritable.get());
    }
}
