package empcl.flink.sql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * Module Desc:
 * 实现kafka topic1到kafka topic2的功能，没有使用flink-connector
 * User: empcl
 * DateTime: 2019/12/16 21:50
 */
public class Kafka2KafkaNoConnectorDemo {
    public static void main(String[] args) throws Exception {
        final String jobName = Kafka2KafkaNoConnectorDemo.class.getSimpleName();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final String TOPIC_IN = "records";
        final String TOPIC_OUT = "recordOut";

        // kafka source
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic(TOPIC_IN)
                        .property("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
                        .property("group.id", "test")
                        .startFromLatest())
                .withFormat(new Json().deriveSchema())
                .withSchema(new Schema().field("record", Types.STRING))
                .inAppendMode()
                .registerTableSource("recordInfo");


        Table sqlQuery = tEnv.sqlQuery("select * from recordInfo");
        DataStream<Row> dataStream = tEnv.toAppendStream(sqlQuery, Row.class);
        dataStream.print();

        tEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic(TOPIC_OUT)
                        .property("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
                        .sinkPartitionerFixed())
                .withFormat(new Json().deriveSchema())
                .withSchema(new Schema().field("record",Types.STRING))
                .inAppendMode()
                .registerTableSink("recordOut");

        tEnv.sqlUpdate("insert into recordOut select record from recordInfo");

        env.execute(jobName);
    }
}





























