package com.atguigu.flink.state;


import com.atguigu.flink.pojo.AppEvent;
import com.atguigu.flink.pojo.ThirdPartyEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 我们可以实现一个实时对账的需求，也就是app的支付操作和第三方的支付操作的一个双流Join。
 * App的支付事件和第三方的支付事件将会互相等待5秒钟，如果等不来对应的支付事件，那么就输出报警信息。
 */
public class Flink12_BillCheck {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //AppEvent: order-1,pay,1000
        SingleOutputStreamOperator<AppEvent> appDs = env.socketTextStream("hadoop102", 8888)
                .map(line -> {

                    String[] split = line.split(",");

                    return new AppEvent(split[0], split[1], Long.parseLong(split[2]));

                }).assignTimestampsAndWatermarks(WatermarkStrategy.<AppEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                );


        // order-1,wechat,pay,2000
        SingleOutputStreamOperator<ThirdPartyEvent> thirdDs = env.socketTextStream("hadoop102", 9999)
                .map(line -> {

                    String[] split = line.split(",");

                    return new ThirdPartyEvent(split[0], split[1], split[2], Long.parseLong(split[3]));

                }).assignTimestampsAndWatermarks(WatermarkStrategy.<ThirdPartyEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                );


        appDs.keyBy(AppEvent::getOrderId)
                .connect(thirdDs.keyBy(ThirdPartyEvent::getOrderId))
                .process(new KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>() {


                    private ValueState<AppEvent> appState;
                    private ValueState<ThirdPartyEvent> thirdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<AppEvent> appStateS = new ValueStateDescriptor<>("appState", AppEvent.class);
                        appState = getRuntimeContext().getState(appStateS);
                        ValueStateDescriptor<ThirdPartyEvent> thirdPartyS = new ValueStateDescriptor<>("thirdPartyS", ThirdPartyEvent.class);
                        thirdState = getRuntimeContext().getState(thirdPartyS);
                    }

                    @Override
                    public void processElement1(AppEvent value, KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        // 尝试从状态中获取ThirdParty
                        if (thirdState.value() != null) {
                            ThirdPartyEvent value1 = thirdState.value();
                            // 对账成功
                            out.collect(value.getOrderId() + "对账成功,third先到，app后到");
                            thirdState.clear();

                            // 删除定时器
                            ctx.timerService().deleteEventTimeTimer(value1.getTs() + 5000L);
                        }else {
                            appState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.getTs() + 5000L);
                        }
                    }

                    @Override
                    public void processElement2(ThirdPartyEvent value, KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>.Context ctx, Collector<String> out) throws Exception {
                       // 尝试从状态中获取app
                        if (appState.value() != null) {
                            AppEvent value1 = appState.value();
                            // 对账成功
                            out.collect(value.getOrderId() + "对账成功,app先到，third后到");


                            // 删除定时器
                            ctx.timerService().deleteEventTimeTimer(value1.getTs() + 5000L);
                            appState.clear();
                        }else {
                            thirdState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.getTs() + 5000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                      //定时器触发对账失败
                        if (appState.value() != null) {
                            out.collect(appState.value().getOrderId() + "对账失败,Third没有到");
                            appState.clear();
                        }
                        if (thirdState.value() != null) {
                            out.collect(thirdState.value().getOrderId() + "对账失败,app没有到");
                            thirdState.clear();
                        }
                    }
                }).print();

        env.execute();
    }

}
