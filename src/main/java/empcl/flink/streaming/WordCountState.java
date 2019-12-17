package empcl.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Module Desc:
 *  keyed state demo
 * User: empcl
 * DateTime: 2019/12/15 23:22
 */
public class WordCountState {
    private static final Logger logger = LoggerFactory.getLogger(WordCountState.class);

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String jobName = WordCountState.class.getSimpleName();
        final int port = 9999;
        final String hostname = "master";
        DataStream<String> wordStream = env.socketTextStream(hostname, port);
        wordStream
                .map(new MapFunction<String, Tuple2<Integer,Integer>>() {
                    public Tuple2 map(String value) {
                        int v = Integer.valueOf(value);
                        return Tuple2.of(v,2);
                    }
                })
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();

        env.execute(jobName);
    }
    
    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Integer,Integer>, Tuple2<Integer, Integer>> {
        // ValueState 状态句柄，第一个值为count，第二个值为sum
        private transient ValueState<Tuple2<Integer,Integer>> sum;

        @Override
        public void flatMap(Tuple2<Integer,Integer> value, Collector<Tuple2<Integer, Integer>> out) throws IOException {
            // 获取当前状态值
            Tuple2<Integer, Integer> currentTime = sum.value();
            // 更新
            currentTime.f0 = currentTime.f0 + 1;
            currentTime.f1 += value.f1;

            // 更新状态值
            sum.update(currentTime);

            // 如果count >= 2,出现两次的时候才计算,清空状态值，重新计算
            if (currentTime.f0 >= 2) {
                out.collect(Tuple2.of(value.f0,currentTime.f1 / currentTime.f0)); // (v,平均值)
                logger.info(Tuple2.of(value.f0,currentTime.f1 / currentTime.f0).toString());
                sum.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("执行几次？");
            ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                    new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                            "average",  // state名称
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                            }),// state类型
                            Tuple2.of(0, 0)
                    );
            sum = getRuntimeContext().getState(descriptor);
        }
    }
}
