package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 有界流,DataStream
 */
public class Flink02_WordCount_Bounded_Stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/word.txt");


        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStreamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatMapStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        keyedStream.sum(1)
                        .print();


        env.execute();
    }
}
