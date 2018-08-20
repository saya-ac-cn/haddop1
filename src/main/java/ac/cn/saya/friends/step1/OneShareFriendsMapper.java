package ac.cn.saya.friends.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Title: OneShareFriendsMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 23:33
 * @Description:
 */

public class OneShareFriendsMapper  extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行 A:B,C,D,F,E,O
        String line = value.toString();

        // 2 切割
        String[] fields = line.split(":");

        // 3 获取 person 和 好友
        String person = fields[0];
        String[] friends = fields[1].split(",");

        // 4 写出去
        for(String friend : friends)
        {
            // 输出 <好友，人>
            k.set(friend);
            v.set(person);
            context.write(k,v);
        }
    }
}
