package ac.cn.saya.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @Title: LogProcessor
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/8 21:36
 * @Description:
 */

public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    // 处理每条消息
    @Override
    public void process(byte[] key, byte[] value) {
        String input = new String (value);
        // 如果包含 >>> 则只保留该标记后面的内容
        if(input.contains(">>>"))
        {
            input = input.split(">>>")[1].trim();
        }
        // 输出到下一个topic
        context.forward(key, input.getBytes());
    }

     // 处理间隔时间段的消息
    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
