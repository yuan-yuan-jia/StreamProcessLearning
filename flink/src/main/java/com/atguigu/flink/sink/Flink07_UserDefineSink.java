package com.atguigu.flink.sink;

import com.alibaba.fastjson2.JSON;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 *
 */
public class Flink07_UserDefineSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.addSink(new SinkFunction<Event>() {
            @Override
            public void invoke(Event value, Context context) throws Exception {
                // 打印到控制台
                System.out.println("event >>>" + JSON.toJSONString(value));
            }
        });

        env.execute();
    }
}
