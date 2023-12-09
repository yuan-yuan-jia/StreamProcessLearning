package com.atguigu.flink.window;

import com.alibaba.fastjson2.JSON;
import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 水位线传递
 * 1. 多个上游，同时给1个下游传递水位线，下游取哪个水位线?
 *   下游取最小的水位线
 * 2. 1个上游，同时给多个下游传递水位线，下游取哪个水位线
 *    上游给下游传递水位线，采用广播的方式将水位线发送给下游（子任务）
 */
public class Flink05_WatermarkTransfer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> ds1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO));


        SingleOutputStreamOperator<Event> ds2 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO));


        ds1.union(ds2)
                        .map(JSON::toJSONString).name("map1")
                        .map(String::toUpperCase).name("map2").setParallelism(2)
                        .print();


        env.execute();
    }

}
