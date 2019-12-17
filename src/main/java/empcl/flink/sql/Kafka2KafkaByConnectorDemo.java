package empcl.flink.sql;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Module Desc:
 * 使用flink-connector-kafka完成kafka topic1到kafka topic2数据的传输
 * User: empcl
 * DateTime: 2019/12/17 11:18
 */
public class Kafka2KafkaByConnectorDemo {
    private static Logger logger = LoggerFactory.getLogger(Kafka2KafkaByConnectorDemo.class);

    private static String JOB_NAME = Kafka2KafkaByConnectorDemo.class.getSimpleName();
    private static String BOOTSTRAP_SERVERS = "master:9092,slave1:9092,slave2:9092";
    private static String GROUP_ID = "testConnector";
    private static String TOPIC_IN = "recordInConnector";
    private static String TOPIC_OUT = "recordOutConnector";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(TOPIC_IN, new SimpleStringSchema(), props);
        flinkKafkaConsumer.setStartFromEarliest();
        DataStream<String> kafkaStream = env.addSource(flinkKafkaConsumer);

        // stream -> table
        // 注册record_in表
        tEnv.registerDataStream("record_in", kafkaStream, "record");
        Table kafkaTable = tEnv.sqlQuery("select record from record_in");

        // kafka connector
        tEnv
                .connect(
                        new Kafka()
                                .version("universal")
                                .topic(TOPIC_OUT)
                                .property("bootstrap.servers", BOOTSTRAP_SERVERS)
                                .sinkPartitionerFixed())
                .withFormat(new Json().deriveSchema())
                .withSchema(new Schema().field("record", Types.STRING))
                .inAppendMode()
                .registerTableSink("record_out");


        kafkaTable.insertInto("record_out");

        env.execute(JOB_NAME);
    }
}
