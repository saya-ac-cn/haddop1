package ac.cn.saya.wordCount.plus;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 这里是mr程序 reduce阶段处理的类
 *
 * KEYIN：就是reduce阶段输入的数据key类型，对应mapper的输出key类型 在本案例中 就是单词 Text
 *
 * VALUEIN：就是reduce阶段输入的数据value类型，对应mapper的输出value类型 在本案例中 就是单词次数 IntWritable
 *
 * KEYOUT：就是reduce阶段输出的数据key类型，对应mapper的输出key类型 在本案例中 就是单词 Text
 *
 * VALUEOUT：就是reduce阶段输出的数据value类型，对应mapper的输出key类型 在本案例中 就是单词的总次数 IntWritable
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    /**
     * 这里是reduce阶段具体业务类的实现方法
     * 接收所有来自map阶段处理的数据之后，按照key的字典进行排序
     * 按照key是否相同作为一组调用reduce方法
     * 本方法的key就是这一组相同key的共同key
     * 把这一组所有的v作为一个迭代器传入reduce方法
     * 框架每传递进来一个kv 组，reduce方法被调用一次
     * <hadoop,[1,1]>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个计数器
        int count = 0;
        //遍历这一组迭代器，，累加到count中
        for(IntWritable value:values){
            count += value.get();
        }
        //把结果输出
        context.write(key, new IntWritable(count));

    }
}
