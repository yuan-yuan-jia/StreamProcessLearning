package com.atguigu.flink.practice;

import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

/**
 * 1.Flink中的时间语义有几种,分别是什么?
 * 两种
 * (1)事件时间
 * (2)处理时间
 * 2.什么是水位线、水位线的生成策略、水位线传递的原则?
 * (1) 水位线是标志一段时间内的数据是否已经全部到达
 * (2) 1. 每到一个元素生成一个水位线 2 周期性生成水位线
 * (3) 1. 上游到下游，下游选择最小的水位线，2.对于多个子任务，上游是广播传递
 * 3.Flink中的窗口有哪些?
 * 1. 时间窗口
 * 2. 计数窗口
 * 3 滚动窗口、滑动窗口、全窗口、会话窗口
 * <p>
 * 4.增量聚合和全窗口函数的区别?
 * 增量聚合： 每来一个元素就进行计算
 * 全窗口函数：窗口结束时，计算能拿到全部的数据
 * 5.编程题:从Kafka topicA中消费Event(json格式)数据,并完成每10秒每URL的点击次数统计,
 * 并将统计结果写出到Mysql url_count 表中,表字段如下:url
 * <p>
 * count window_start window_end
 */
public class EventToMySQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setTopics("topicA")
                .setGroupId("finkd")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        SinkFunction<UrlViewCount> sink = JdbcSink.<UrlViewCount>sink("insert into  url_count(url,cnt,window_start,window_end) values (?,?,?,?) ", new JdbcStatementBuilder<UrlViewCount>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, UrlViewCount urlViewCount) throws SQLException {
                        preparedStatement.setString(1, urlViewCount.getUrl());
                        preparedStatement.setLong(2, urlViewCount.getCount());
                        preparedStatement.setLong(3, urlViewCount.getWindowStart());
                        preparedStatement.setLong(4, urlViewCount.getWindowEnd());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(2)
                        .withBatchSize(200)
                        .withBatchIntervalMs(2000L)
                        .build()
                ,new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUsername("root")
                        .withPassword("root")
                        .withUrl("jdbc:mysql://localhost:3306/demo0717")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withConnectionCheckTimeoutSeconds(2)
                        .build()
        );

        SingleOutputStreamOperator<String> ds1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        ds1.print("kafka");

        SingleOutputStreamOperator<UrlViewCount> ds=   ds1    .map(line -> {

                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })).map(e -> new WordCount(e.getUrl(), 1))
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount value) throws Exception {
                        return value.getWord();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                        return new WordCount(value1.getWord(), value2.getCount() + value1.getCount());
                    }
                }, new ProcessWindowFunction<WordCount, UrlViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WordCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<WordCount> elements, Collector<UrlViewCount> out) throws Exception {
                        WordCount next = elements.iterator().next();
                        UrlViewCount urlViewCount = new UrlViewCount();

                        urlViewCount.setUrl(s);
                        urlViewCount.setCount((long) next.getCount());
                        urlViewCount.setWindowStart(context.window().getStart());
                        urlViewCount.setWindowEnd(context.window().getEnd());
                        out.collect(urlViewCount);
                    }
                });

        ds.print();
        ds.addSink(sink);


        env.execute();
    }

}
