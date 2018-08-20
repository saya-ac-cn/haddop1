package ac.cn.saya.friends.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Title: TwoShareFriendsReducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/21 00:07
 * @Description:
 */

public class TwoShareFriendsReducer  extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();

        //  <人-人，共同好友>
        for (Text friend : values) {
            sb.append(friend).append(" ");
        }
        context.write(key, new Text(sb.toString()));

    }
}
