package com.atguigu.flink.transform;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 函数的实现方式
 * 1. 自定义类，实现接口
 *   编码麻烦，使用灵活
 * 2. 匿名函数
 *   编码简单
 * 3. Lambda
 *   编码简洁
 */
public class Flink02_FunctionImplement {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 8888);
        // 匿名内部类实现
//        dataStreamSource.flatMap(new FlatMapFunction<String, WordCount>() {
//            @Override
//            public void flatMap(String value, Collector<WordCount> out) throws Exception {
//                for (String s : value.split(" ")) {
//                    out.collect(new WordCount(s,1));
//                }
//            }
//        }).keyBy(new KeySelector<WordCount, String>() {
//            @Override
//            public String getKey(WordCount value) throws Exception {
//                return value.getWord();
//            }
//        }).sum("count").print();


        // 自定义类
        dataStreamSource.flatMap(new MyFlatMapFunction(" "))
                        .keyBy(WordCount::getWord).sum("count")
                        .print("自定义函数");
        env.execute();
    }


    public static class MyFlatMapFunction implements FlatMapFunction<String,WordCount> {

        private final String splitSymbol;
        public MyFlatMapFunction(String splitSymbol) {
            if (splitSymbol == null) {
                throw new IllegalArgumentException("splitSymbol is not allowed to be null");
            }
            this.splitSymbol = splitSymbol;
        }



        @Override
        public void flatMap(String value, Collector<WordCount> out) throws Exception {
            for (String s : value.split(splitSymbol)) {
                out.collect(new WordCount(s,1));
            }
        }
    }


}
