package ac.cn.saya.flowsum.partitioner;

import ac.cn.saya.flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 自定义分区
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    public static HashMap<String,Integer> provinceMap = new HashMap<>();

    static {
        provinceMap.put("134",0);
        provinceMap.put("135",1);
        provinceMap.put("136",2);
        provinceMap.put("137",3);
        provinceMap.put("138",4);
    }

    //分区方法
    @Override
    public int getPartition(Text key, FlowBean value, int i) {
        //获取电话号码的前3位
        Integer code = provinceMap.get(key.toString().substring(0,3));
        if(code != null)
        {
            return code;
        }
        return 4;
    }
}
