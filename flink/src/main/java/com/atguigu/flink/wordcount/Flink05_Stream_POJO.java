package com.atguigu.flink.wordcount;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
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
public class Flink05_Stream_POJO {
    public static void main(String[] args) throws Exception {




        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置并行度
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/word.txt");


        SingleOutputStreamOperator<WordCount> flatMapStreamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    WordCount wordCount = new WordCount();
                    wordCount.setWord(s);
                    wordCount.setCount(1);
                    out.collect(wordCount);
                }
            }
        });

        flatMapStreamOperator.keyBy(new KeySelector<WordCount, String>() {
            @Override
            public String getKey(WordCount value) throws Exception {
                return value.getWord();
            }
        }).sum("count")
                        .print();



        env.execute();

    }
}
