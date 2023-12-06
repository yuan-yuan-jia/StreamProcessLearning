package com.atguigu.flink.transform;

import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 调用算子的时候，需要传入用户自定义函数
 * 来完成具体的功能
 * 函数：
 * 1. 普通函数
 *    MapFunction
 *    FilterFunction
 *    FlatMapFunction
 *    ReducerFunction
 *    ....
 * 2. 富函数
 *   基本每个普通函数都有对应的富函数:
 *   结构
 *   RichFunction -> AbstractRichFunction -> 具体使用的富函数类
 *   RichMapFunction
 *   RichFlatMapFunction
 *   RichFilterFunction
 *   ...
 *   功能：
 *   1. 生命周期方法：
 *     open(): 当前算子每个并行子任务创建时会调用一次
 *     close():  并行子任务销毁时会调用一次（有界流会销毁）
 *   2. 获取运行时上下文对象：RuntimeContext
 *      (1) 获取当前作业，当前task相关的信息
 *      (2) getState getListState getReducingState getAggregatorState getMapState
 *          获取不同的状态，作状态编程****
 *      (3)
 *
 */
public class Flink05_RichFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);


        ds.map(new RichMapFunction<Event, WordCount>() {

            // 生命周期open方法
            // 当前算子实例创建时，执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("创建连接对象");
            }

            @Override
            public WordCount map(Event value) throws Exception {

                System.out.println("每条数据");
                return new WordCount(value.getUser(), 1);
            }

            // 当前算子实例销毁时执行一次
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("关闭连接对象");
            }
        }).print();


        env.execute();
    }

}
