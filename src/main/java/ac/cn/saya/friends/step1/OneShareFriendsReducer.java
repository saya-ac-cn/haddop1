package ac.cn.saya.friends.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title: OneShareFriendsReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 23:40
 * @Description:
 */

public class OneShareFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();

        // 1 拼接
        for (Text person : values)
        {
            sb.append(person).append(",");
        }

        //2 写出 A I,K,C,B,G,F,H,O,D,
        context.write(key, new Text(sb.toString()));
    }
}
