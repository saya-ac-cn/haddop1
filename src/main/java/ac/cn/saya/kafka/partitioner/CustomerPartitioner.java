package ac.cn.saya.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Title: CustomerPartitioner
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/6 21:34
 * @Description:
 */

public class CustomerPartitioner implements Partitioner {

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
