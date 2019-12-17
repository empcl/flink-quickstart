package empcl.flink.streaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Module Desc:
 *  从kafka topic处消费数据
 * User: empcl
 * DateTime: 2019/12/17 16:58
 */
public class FlinkKafkaConsumerDemo {
    private static String JOB_NAME = FlinkKafkaConsumerDemo.class.getSimpleName();
    private static String BOOTSTRAP_SERVERS = "master:9092,slave1:9092,slave2:9092";
    private static String TOPIC = "recordOutConnector";
    private static String GROUP_ID = "testConsumer";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers",BOOTSTRAP_SERVERS);
        props.put("group.id",GROUP_ID);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(TOPIC, new SimpleStringSchema(), props);
        DataStream<String> kafkaStream = env.addSource(consumer);
        kafkaStream.print();

        env.execute(JOB_NAME);
    }
}
