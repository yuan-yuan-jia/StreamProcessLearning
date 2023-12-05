package com.atguigu.flink.source;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_EventSource {

    public static DataStream<Event> getEventSource(StreamExecutionEnvironment env) {

        DataGeneratorSource<Event> eventDataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, Event>() {


            @Override
            public Event map(Long value) throws Exception {
                String[] users = {"zhangsan", "lisi", "Tom", "Jerry"};
                String[] urls = {"/home", "/detail", "/login", "/order", "/pay"};
                String user = users[RandomUtils.nextInt(0, users.length)];
                String url = urls[RandomUtils.nextInt(0, urls.length)];
                Event event = new Event(user, url, System.currentTimeMillis());

                return event;
            }
        }, 100000L, RateLimiterStrategy.perSecond(2), Types.POJO(Event.class)
        );
        return env.fromSource(eventDataGeneratorSource, WatermarkStrategy.noWatermarks(),"df");
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> eventSource = getEventSource(env);
        eventSource.print();

        env.execute();
    }
}
