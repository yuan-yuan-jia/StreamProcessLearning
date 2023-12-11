package com.atguigu.flink.window;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * WindowJoin: 在同一个窗口内相同key的数据才能join
 */
public class Flink16_WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //order-1,1000
        SingleOutputStreamOperator<OrderEvent> ds1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderEvent(split[0], Long.parseLong(split[1]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e,l) -> e.getTs())
                );

        //detail-1,order-1,1000
        SingleOutputStreamOperator<OrderDetailEvent> ds2 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderDetailEvent(split[0], split[1],  Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetailEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e,l) -> e.getTs())
                );

        // window join
        ds1.join(ds2)
                // 第一条流用于join的key
                .where(OrderEvent::getOrderId)
                // 第二条流用于join的key
                .equalTo(OrderDetailEvent::getOrderId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<OrderEvent, OrderDetailEvent, String>() {
                    @Override
                    public String join(OrderEvent first, OrderDetailEvent second) throws Exception {
                       // 处理Join成功的数据
                        return first + "---> " + second;
                    }
                }).print("window");

        env.execute();

    }
}
