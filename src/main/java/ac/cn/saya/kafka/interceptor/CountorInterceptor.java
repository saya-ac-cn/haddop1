package ac.cn.saya.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Title: CountorInterceptor
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/7 21:25
 * @Description:
 * 统计成功和失败发送的次数
 */

public class CountorInterceptor implements ProducerInterceptor<String, String> {

    private long successCount = 0;
    private long erroCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null)
        {
            // 未发生异常
            successCount++;
        }
        else
        {
            erroCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("发送成功的个数：" + successCount);
        System.out.println("发送失败的个数：" + erroCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
