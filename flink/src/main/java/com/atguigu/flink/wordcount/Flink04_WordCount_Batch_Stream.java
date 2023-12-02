package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 流批一体
 * (DataSet) WordCount
 * Flink程序的编码套路
 * 1. 创建运行环境（批处理、流处理、表处理）
 * 2. 对接数据源，读取数据
 * 3. 对数据进行转换处理
 * 4. 将结果写出
 */
public class Flink04_WordCount_Batch_Stream {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置运行模式
        /*
             STREAMING: 流模式，默认的
             BATCH: 批模式
             AUTOMATIC: 如果数据源是有界的，使用批模式
         */
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

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
