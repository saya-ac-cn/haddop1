package ac.cn.saya.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Title: CustomConsumer1
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/6 21:37
 * @Description:
 */

public class CustomConsumer1 {

    public static void main(String[] args)
    {
        // 1、配置消费者属性
        Properties props = new Properties();
        // 定义kafka的服务器地址
        props.put("bootstrap.servers","DataNode1:9092");
        // 设置消费组
        props.put("group.id","saya");
        // 是否自动确认
        props.put("enable.auto.commit","true");
        // key的反序列化
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // value的反序列化
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // 2、创建消费者实例
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅消息主题
        consumer.subscribe(Arrays.asList("saya"));
        // 虚拟机关机时执行->释放资源
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if(consumer != null)
                {
                    consumer.close();
                }
            }
        }));
        // 3、拉消息
        while (true)
        {
            // 100最大时间间隔
            ConsumerRecords<String,String > records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records)
            {
                System.out.println(record.offset() + "---" + record.key() + "----" + record.value());
            }
        }
    }

}
