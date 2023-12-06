package com.atguigu.flink.transform;

import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单聚合算子
 * sum min minBy max maxBy
 * 原则： 要聚合，先KeyBy
 */
public class Flink03_SimpleAggOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);
        ds.print("input");

        //sum: 求每个url的点击次数
        ds.map(e -> new WordCount(e.getUrl(), 1))
                .keyBy(WordCount::getWord)
                .sum("count");
        //.print("sum");
        //min: 求每个url ts最小的数据

        ds.keyBy(Event::getUrl)
                .min("ts");
                //.print("min");
        // 根据最小字段数据，来取整条数据
        ds.keyBy(Event::getUrl)
                .minBy("ts")
                .print("minBy");


        env.execute();
    }

}
