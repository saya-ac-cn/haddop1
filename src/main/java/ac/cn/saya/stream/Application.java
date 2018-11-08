package ac.cn.saya.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @Title: Application
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/11/8 21:53
 * @Description:
 */

public class Application {

    public static void main(String[] args)
    {
        // 定义输入的topic
        String fromTopic = "test2";
        // 定义输出的topic
        String toTopic = "test3";

        // 设置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"process");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"DataNode1:9092,DataNode2:9092,NameNode:9092");
        StreamsConfig config = new StreamsConfig(settings);

        // 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE",fromTopic)
                .addProcessor("PROCESS", new ProcessorSupplier<byte[],byte[]>() {
                    @Override
                    public Processor get() {
                        // 具体分析处理
                        return new LogProcessor();
                    }
                },"SOURCE")
                    .addSink("SINK",toTopic,"PROCESS");

        // 创建kafka strame
        KafkaStreams streams = new KafkaStreams(builder,config);
        streams.start();
    }

}
