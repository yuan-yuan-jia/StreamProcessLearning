package com.atguigu.flink.practice;

import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 1. Flink如何处理迟到数据
 * 推迟水位线推进、延迟窗口关闭、侧输出流处理
 * <p>
 * 2. windowJoin 和 IntervalJoin的工作原理
 * windowJoin： 在同一个窗口内且key相同的数据进行join
 * IntervalJoin：一条流的数据为基准，在一定时间间隔内的数据进行join
 * 3. 处理函数的功能
 * (1) 注册定时器
 * (2) 处理定时任务
 * (3) 自定义处理每个数据的逻辑
 * 4. 编程题:基于生成器生成Event数据,求每10秒内每个ur1的点击次数Top2,将结果写出到Mysq1表中。
 */
public class Top2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((e, l) -> e.getTs())
                );



        SingleOutputStreamOperator<UrlViewCount> resultStream = ds.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Event, UrlViewCount, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Event, UrlViewCount, TimeWindow>.Context context, Iterable<Event> elements, Collector<UrlViewCount> out) throws Exception {


                        Map<String, Long> urlCountMap = new HashMap<>();
                        for (Event element : elements) {
                            String url = element.getUrl();
                            Long count = urlCountMap.getOrDefault(url, 0L);
                            urlCountMap.put(url, count + 1);
                        }

                        long start = context.window().getStart();
                        long end = context.window().getEnd();


                        List<UrlViewCount> urlViewCounts = urlCountMap.entrySet().stream().map(e -> {
                            return new UrlViewCount(e.getKey(), e.getValue(), start, end);
                        }).collect(Collectors.toList());

                        urlViewCounts.sort(new Comparator<UrlViewCount>() {
                            @Override
                            public int compare(UrlViewCount o1, UrlViewCount o2) {
                                if (o1.getCount() < o2.getCount()) {
                                    return 1;
                                } else if (o1.getCount() > o2.getCount()) {
                                    return -1;
                                } else {
                                    return 0;
                                }
                            }
                        });

                        for (int i = 0; i < 2 && i < urlViewCounts.size(); i++) {
                            out.collect(urlViewCounts.get(i));
                        }

                    }
                });

        resultStream.print("result");


        SinkFunction<UrlViewCount> jdbcSink = JdbcSink.<UrlViewCount>sink("insert into url_count(url,cnt,window_start,window_end) values(?,?,?,?)",
                new JdbcStatementBuilder<UrlViewCount>() {

                    @Override
                    public void accept(PreparedStatement preparedStatement, UrlViewCount urlViewCount) throws SQLException {
                        preparedStatement.setString(1, urlViewCount.getUrl());
                        preparedStatement.setLong(2,urlViewCount.getCount());
                        preparedStatement.setLong(3,urlViewCount.getWindowStart());
                        preparedStatement.setLong(4,urlViewCount.getWindowEnd());
                    }
                }, JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(200)
                        .withBatchSize(200)
                        .withMaxRetries(2)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/demo0717")
                        .withUsername("root")
                        .withPassword("root")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withConnectionCheckTimeoutSeconds(20)
                        .build()

        );

        resultStream.addSink(jdbcSink);


        env.execute();
    }
}
