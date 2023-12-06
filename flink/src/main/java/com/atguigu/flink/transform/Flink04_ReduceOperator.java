package com.atguigu.flink.transform;

import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * reduce: 归约聚合
 * 聚合原理： 两两聚合，上一次的聚合值与本次新到的数据进行聚合
 * ReduceFunction:
 * 泛型：T:流中的数据类型
 * 方法: T reduce(T value1,T value2) throws Exception
 * value1: 上一次的聚合值
 * value2: 本次新到的值
 * 要求输入和输出的类型一致。
 *  每个key的第一个输入数据不参与聚合，直接输出
 */
public class Flink04_ReduceOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        // reduce: 每个用户的点击次数
        ds.map(e -> new WordCount(e.getUser(), 1))
                .keyBy(WordCount::getWord)
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                        System.out.println("reduce...");
                        return new WordCount(value1.getWord(), value1.getCount() + value2.getCount());
                    }
                }).print("reduce");

        env.execute();
    }
}
