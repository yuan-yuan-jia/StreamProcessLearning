package com.atguigu.flink.window;

import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Flink内置的水位线生成策略
 * WatermarkStrategy.forMonotonousTimestamps(): 有序流周期性
 * WatermarkStrategy.forBoundedOutOfOrderness(Duration maxOutOfOrderness): 乱序周期性
 */
public class Flink04_FlinkProvideWatermarkStrategy {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(500);

        //tom,/home,1000
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {

                    String[] split = line.split(",");
                    Event event = new Event();
                    event.setUser(split[0]);
                    event.setUrl(split[1]);
                    event.setTs(Long.parseLong(split[2]));
                    return event;

                });
        // 生成水位线
        SingleOutputStreamOperator<Event> watermarkDs = ds.assignTimestampsAndWatermarks(
                // 有序流
                /*
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        // 时间戳提取
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })

                 */
                // 乱序
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(200))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );


        watermarkDs.print();


        env.execute();
    }

}
