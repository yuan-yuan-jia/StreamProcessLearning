package com.atguigu.flink.deployment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * WordCount:无界流
 */
public class Flink01_WordCount_Unbounded_Stream {

    public static void main(String[] args) throws Exception {
        System.out.println("Flink01_WordCount_Unbounded_Stream main");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如何给程序传参
        /// 1 通过固定位置的方式
        /// 2 通过键值方式

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostName = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        // 无界流：网络端口、Kafka等
        DataStreamSource<String> ds = env.socketTextStream(hostName, port);

        ds.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(Tuple2.of(s,1));
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1)
                .print();

        env.execute();
    }
}
