package ac.cn.saya.flowsum.sort;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartitioner extends Partitioner<FlowSortBean, Text> {

    public static HashMap<String, Integer> provinceMap = new HashMap<>();

    static {
        provinceMap.put("134", 0);
        provinceMap.put("135", 1);
        provinceMap.put("136", 2);
        provinceMap.put("137", 3);
        provinceMap.put("138", 4);
    }

    //分区方法
    @Override
    public int getPartition(FlowSortBean key, Text value, int i) {
        //获取电话号码的前3位
        Integer code = provinceMap.get(value.toString().substring(0, 3));
        if (code != null) {
            return code;
        }
        return 4;
    }
}
