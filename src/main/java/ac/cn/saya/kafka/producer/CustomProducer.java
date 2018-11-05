package ac.cn.saya.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Title: CustomProducer
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/5 22:01
 * @Description:
 * kafka生产者实例
 */

public class CustomProducer {

    public static void main(String[] args)
    {
        // 1、配置生产者属性
        Properties props = new Properties();
        // 配置kafka集群节点的地址，可以是多个
        props.put("bootstrap.servers","DataNode1:9092");
        // 配置发送的消息是否等待应答
        props.put("acks","all");
        // 配置消息发送失败的重试
        props.put("retries","0");
        // 批量处理数据的大小：16k
        props.put("batch.size","16384");
        // 设置批处理数据的延迟，单位：ms
        props.put("linger.ms","5");
        // 设置内存缓冲区的大小
        props.put("buffer.memory","33554432");
        // 在发送前序列化
        // key
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // value
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 2、实例化KafkaProducer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        for(int i = 0; i< 50; i++)
        {
            // 调用Producer的send方法，进行消息的发送，每条待发送的消息，都必须封装为一个Recordduix
            producer.send(new ProducerRecord<String,String>("saya","hi,saya.from:"+ i));
        }

        System.out.println("发送完毕");
        // 4、关闭资源
        producer.close();
    }

}
