package com.atguigu.flink.process;

import com.alibaba.fastjson2.JSON;
import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 定时服务和定时器
 * TimerService: 定时服务，用于注册定时器，删除定时器等
 * Timer：定时器，在未来的某个时间注册一个事件,
 * 定时器触发，会执行定义的事件。
 * process中的onTimer:定时触发后，会自动调用该方法
 *
 * 注意：
 *   1. 要定时，线keyBy，只有KeyedStream才能注册定时器，未来定时器会按照key
 * 进行隔离
 *   2. 同一个key在注册多个相同时间定时器，未来只会触发一次
 */
public class Flink02_TimerAndTimerService {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)

                .map(line -> {
                    String[] split = line.split(",");

                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                                Duration.ZERO
                        ).withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        ds.print("input");
        ds.keyBy(Event::getUser)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {


                        TimerService timerService = ctx.timerService();
                       // long currentProcessingTime = timerService.currentProcessingTime();
                        //long futureTime = currentProcessingTime + Time.seconds(10).toMilliseconds();
                        //System.out.println("当前处理时间:" + currentProcessingTime);
                        //注册处理时间定时
                       //timerService.registerProcessingTimeTimer(futureTime);

                        // timerService.registerEventTimeTimer();
                        long futureTime = value.getTs() + Time.seconds(10).toMilliseconds();
                        System.out.println("注册处理时间>>> " + futureTime);
                        timerService.registerEventTimeTimer(futureTime);

                        out.collect(JSON.toJSONString(value));
                    }

                    // 定时器触发后，会调用该方法
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        //System.out.println("触发了处理时间定时器>>> " + timestamp);
                        System.out.println("触发了事件时间定时器>>> " + timestamp);
                        System.out.println("当前水位线:" + ctx.timerService().currentWatermark());
                    }
                }).print("process");


        env.execute();
    }
}
