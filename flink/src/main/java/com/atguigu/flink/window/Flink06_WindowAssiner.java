package com.atguigu.flink.window;

import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口分配器：
 * 非按键分区：所有数据不会进行key的隔离，都往同一个窗口放
 * 按键分区： 数据会按照key进行隔离，会为每个key分配对应的窗口
 */
public class Flink06_WindowAssiner {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.print("input");

        // 非按键分区
        // 计数窗口
        ds.map(e -> Tuple1.of(1L))
                .returns(Types.TUPLE(Types.LONG))
                .countWindowAll(5L)
                .sum(0);
        //.print("window");

        // 计数滑动窗口
        ds.map(e -> Tuple1.of(1L))
                .returns(Types.TUPLE(Types.LONG))
                .countWindowAll(5L, 3L)
                .sum(0);
                //.print("window");


        SingleOutputStreamOperator<Event> dsWithWaterMark = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner((e, t) -> e.getTs()));

        // 时间滚动窗口
        dsWithWaterMark.map(e -> Tuple1.of(1L))
                .returns(Types.TUPLE(Types.LONG))
                .windowAll(
                        // 处理时间
                        //TumblingProcessingTimeWindows.of(Time.seconds(5))
                        // 事件时间
                        TumblingEventTimeWindows.of(Time.seconds(5))
                ).sum(0);//.print("window");


        // 时间滑动窗口
        dsWithWaterMark.map(e -> Tuple1.of(1L))
                .returns(Types.TUPLE(Types.LONG))
                .windowAll(
                        // 处理时间
                       // SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))
                        // 事件时间
                        SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))
                ).sum(0);//.print("window");

        // 时间会话
        dsWithWaterMark.map(e -> Tuple1.of(1L))
                .returns(Types.TUPLE(Types.LONG))
                .windowAll(
                        // 处理时间
                        //ProcessingTimeSessionWindows.withGap(Time.seconds(3))
                        // 事件时间
                        EventTimeSessionWindows.withGap(Time.seconds(3))
                ).sum(0).print("window");


        env.execute();
    }

}
