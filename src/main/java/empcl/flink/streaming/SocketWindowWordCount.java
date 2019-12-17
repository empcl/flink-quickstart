package empcl.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Module Desc:
 *  WC streaming base on socket
 * User: empcl
 * DateTime: 2019/11/30 22:26
 */
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception{
        final String jobName = SocketWindowWordCount.class.getSimpleName();
        final int port;
        final String path;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
            path = params.get("path");
        } catch (Exception e) {
            System.out.println("No port specified.Please run 'SocketWindowWordCount --port <port>'");
            return;
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socketStream = env.socketTextStream("master", port);
        DataStream<Tuple2<String, Integer>> wcStream = socketStream
                .flatMap(new SplittedFunction())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .reduce(new CountFunction());

//        wcStream.print().setParallelism(1);
        wcStream.writeAsText(path, FileSystem.WriteMode.OVERWRITE);

        env.execute(jobName);
    }

    static class SplittedFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String v:value.split(" ")) {
                out.collect(Tuple2.of(v,1));
            }
        }
    }

    static class CountFunction implements ReduceFunction<Tuple2<String,Integer>> {
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
            return Tuple2.of(v1.f0,v1.f1 + v2.f1);
        }
    }
}
