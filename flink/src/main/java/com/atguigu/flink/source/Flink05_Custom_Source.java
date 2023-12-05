package com.atguigu.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据源
 */
public class Flink05_Custom_Source {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> clickDs = env.addSource(new ClickSource());
        clickDs.print();

        env.execute();
    }


}
