package ac.cn.saya.friends.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Title: TwoShareFriendsMapper
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 23:53
 * @Description:
 */

public class TwoShareFriendsMapper  extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // A I,K,C,B,G,F,H,O,D,
        // 友 人，人，人
        String line = value.toString();
        String[] friend_persons = line.split("\t");

        String friend = friend_persons[0];
        String[] persons = friend_persons[1].split(",");
        Arrays.sort(persons);

        /**
         * A B C D
         * 两项组合C 2 4
         * A
         *      B
         *      C
         *      D
         * B
         *      C
         *      D
         * C
         *      D
         */
        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                // 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同 1 个 reduce 中去
                context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
            }
        }
    }
}
