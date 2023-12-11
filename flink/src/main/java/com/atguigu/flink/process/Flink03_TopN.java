package com.atguigu.flink.process;

import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

/**
 * 网站中一个非常经典的例子，就是实时统计一段时间内的热门url。
 * 例如，需要统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次。
 * 我们知道，这可以用一个滑动窗口来实现，而“热门度”一般可以直接用访问量来表示。
 * 于是就需要开滑动窗口收集url的访问数据，按照不同的url进行统计，
 * 而后汇总排序并最终输出前两名。这其实就是著名的“Top N”问题。
 * <p>
 * 方案一：不进行keyby，将所有的url的数据往一个窗口中收集，并且使用全量聚合
 * 等到窗口窗口触发计算时，在处理函数中对窗口内所有的数据进行汇总处理
 */
public class Flink03_TopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> ds = Flink06_EventSource.getEventSource(env)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, l) -> e.getTs())
                );

        ds.print("input");

        ds.windowAll(
                        SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))
                        //TumblingEventTimeWindows.of(Time.seconds(10))

                )
                .process(new ProcessAllWindowFunction<Event, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Event, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        // 统计每个url的点击次数
                        HashMap<String, Long> urlCountMap = new HashMap<>();
                        for (Event element : elements) {
                            Long l = urlCountMap.get(element.getUrl());
                            if (l == null) {
                                l = 0L;
                            }
                            l++;
                            urlCountMap.put(element.getUrl(), l);
                        }
                        // 将map结构转成list结构
                        ArrayList<UrlViewCount> urlViewCounts = new ArrayList<>(urlCountMap.size());
                        urlCountMap.entrySet().stream()
                                .map(e -> new UrlViewCount(e.getKey(),e.getValue(),start,end))
                                .forEach(urlViewCounts::add);

                        urlViewCounts.sort(new Comparator<UrlViewCount>() {
                            @Override
                            public int compare(UrlViewCount o1, UrlViewCount o2) {
                                return Long.compare(o2.getCount(),o1.getCount());
                            }
                        });

                        StringBuilder result = new StringBuilder("**********");
                        result.append('\n');
                        // 取topN
                        for (int i = 0; i < urlViewCounts.size() && i < 2; i++) {

                            UrlViewCount urlViewCount = urlViewCounts.get(i);
                            result.append("TOP." + (i + 1) + " " + urlViewCount + "\n");

                        }
                        result.append("*******\n");

                        // 输出
                        out.collect(result.toString());

                    }
                }).print("topN");


        env.execute();
    }

}
