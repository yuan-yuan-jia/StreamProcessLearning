package com.atguigu.flink.window;

import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 *
 */
public class Flink15_WithIdleness {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<Event> ds1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2])
                    );
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                        //设置空闲时间，超过该时间，下游不会采取该水位线
                        .withIdleness(Duration.ofSeconds(2))
                );

        SingleOutputStreamOperator<Event> ds2 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2])
                    );
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                        //设置空闲时间，超过该时间，下游不会采取该水位线
                        .withIdleness(Duration.ofSeconds(2))
                );

        ds1.union(ds2)
                .map(e -> new WordCount(e.getUrl(), 1))
                .keyBy(WordCount::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum("count")
                .print();

        env.execute();
    }
}
