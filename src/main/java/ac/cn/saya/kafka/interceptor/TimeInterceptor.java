package ac.cn.saya.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Title: TimeInterceptor
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/7 21:19
 * @Description:
 * 在发送前拦截，统一加上时间戳
 */

public class TimeInterceptor implements ProducerInterceptor<String, String>{

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<String, String>(
                producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.key(),
                System.currentTimeMillis() + "-" + producerRecord.value()
        );
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
