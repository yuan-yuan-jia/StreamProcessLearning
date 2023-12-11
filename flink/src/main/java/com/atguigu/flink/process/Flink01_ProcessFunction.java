package com.atguigu.flink.process;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 处理函数
 * 1. 功能
 *   (1) 拥有富函数的功能
 *       生命周期方法
 *       状态编程
 *   (2) 对元素的处理功能processElement，在不同的处理函数中，该方法的名字略有区别
 *   (3) 定时器编程：
 *       TimerService:定时服务，可以用于注册定时器，删除定时器等。
 *       onTimer(): 定时器触发后，会自动调用该方法，可以将需要完成的工作
 *       写到该方法
 *   (4)  使用侧输出流
 *
 * 2. 分类：
 * (1) ProcessFunction
 * DataStream => process(ProcessFunction)
 * (2) KeyedProcessFunction
 * KeyedStream => process(KeyedProcessFunction)
 * (3) ProcessWindowFunction
 * WindowStream => process(ProcessWindowFunction)
 * (4) PrcessAllWindowFunction
 * AllWindowStream => process(PrcessAllWindowFunction)
 * (5)CoProcessFunction
 * ConnectedStream => process(CoProcessFunction)
 * (6)ProcessJoinFunction
 * IntervalJoined => process(ProcessJoinFunction)
 * (7) BroadcastProcessFunction
 * BroadcastConnectedStream => process(BroadcastProcessFunction)
 * (8) KeyedBroadcastProcessFunction
 * BroadcastConnectedStream => process(KeyedBroadcastProcessFunction)
 * (9) ...
 *
 */
public class Flink01_ProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.execute();
    }
}
