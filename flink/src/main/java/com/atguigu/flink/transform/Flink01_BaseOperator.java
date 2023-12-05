package com.atguigu.flink.transform;

import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基本转换算子
 * 1. map: 映射，将输入数据处理成另外一种格式输出（一对一）
 * 2. filter: 过滤，将满足过滤条件的数据输出
 * 3. flatMap: 将输入数据进行拆解，并转换格式输出
 */
public class Flink01_BaseOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> eventSource = Flink06_EventSource.getEventSource(env);

        // map: 将每个event中的user提取
        eventSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.getUser();
            }
        });//.print("map");

        // filter: 过滤user=等于Zhangsan和Lisi的数据
        eventSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return "zhangsan".equals(value.getUser()) ||
                        "lisi".equals(value.getUser());
            }
        });
        //.print("filter");


        // flatMap: 将每个Event中的每个字段单独输出
        eventSource.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                String user = value.getUser();
                String url = value.getUrl();
                String ts = String.valueOf(value.getTs());

                out.collect("user:" + user);
                out.collect("url:" + url);
                out.collect("ts:" + ts);

            }
        })
        .print("flatMap");

        env.execute();
    }
}
