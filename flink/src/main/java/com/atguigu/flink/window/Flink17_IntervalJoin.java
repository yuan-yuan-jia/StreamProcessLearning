package com.atguigu.flink.window;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 以一条流中数据为基准，设定上界和下界，形成一个时间范围
 * ，另外一条流中相同key数据在这个时间范围内即可join成功
 */
public class Flink17_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //order-1,1000
        SingleOutputStreamOperator<OrderEvent> ds1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderEvent(split[0], Long.parseLong(split[1]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                );

        //detail-1,order-1,1000
        SingleOutputStreamOperator<OrderDetailEvent> ds2 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderDetailEvent(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetailEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                );

        // interval  join
        ds1.keyBy(OrderEvent::getOrderId)
                .intervalJoin(ds2.keyBy(OrderDetailEvent::getOrderId))
                .between(Time.seconds(-2), Time.seconds(2))
                //排除上边界值
                //.upperBoundExclusive()
                // 排除下边界值
                //.lowerBoundExclusive()
                .process(new ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>() {
                    @Override
                    public void processElement(OrderEvent left, OrderDetailEvent right, ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        String s = left + "--> " + right;
                        out.collect(s);
                    }
                }).print("Interval Join");

        env.execute();

    }
}
