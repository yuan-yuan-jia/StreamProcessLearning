package com.atguigu.flink.transform;

import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * process算子
 * flink 提供的最灵活的算子，需要传入自定义的处理函数(ProcessFunction)
 * ProcessFunction有很多不同的实现，但基本功能差不多
 * 功能：
 *   1. 继承自AbstractRichFunction，拥有富函数的功能
 *     (1) 生命周期方法open close
 *     (2) 状态编程
 *   2. processElement()  每个元素的处理由程序员来实现
 *   3. 定时器编程
 *      onTimer(): 定时器触发后，会自动调用该方法，需要提前在该方法中定义需要处理的逻辑
 *      TimerService: 定时器服务，用户定时的操作，如注册定时器，删除定时器
 *   4. 使用侧输出流：
 *       output():
 */
public class Flink07_ProcessOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);



        env.execute();
    }

}
