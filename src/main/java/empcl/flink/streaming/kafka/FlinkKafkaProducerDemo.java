package empcl.flink.streaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Module Desc:
 *  生产数据到topic中
 * User: empcl
 * DateTime: 2019/12/17 16:48
 */
public class FlinkKafkaProducerDemo {
    private static String JOB_NAME = FlinkKafkaProducerDemo.class.getSimpleName();
    private static String BOOTSTRAP_SERVERS = "master:9092,slave1:9092,slave2:9092";
    private static String TOPIC = "recordInConnector";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketStream = env.socketTextStream("master", 9999);

        Properties props = new Properties();
        props.put("bootstrap.servers",BOOTSTRAP_SERVERS);

        FlinkKafkaProducer<String> producerStream = new FlinkKafkaProducer<String>(TOPIC, new SimpleStringSchema(), props);

        socketStream.addSink(producerStream);

        env.execute(JOB_NAME);
    }
}
