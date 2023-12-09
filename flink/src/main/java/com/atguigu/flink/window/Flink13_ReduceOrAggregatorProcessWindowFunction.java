package com.atguigu.flink.window;

import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.pojo.UserViewCount;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 增量聚合和全量聚合的结合使用：
 * 聚合的过程使用增量聚合函数来完成，
 * 窗口的信息使用全窗口函数来获取。
 * <p>
 * 网站的各种统计指标中，一个很重要的统计指标就是热门的链接；想要得到热门的url，
 * 前提是得到每个链接的“热门度”。一般情况下，可以用url的浏览量（点击量）表示热门度。
 * 我们这里统计10秒钟的url浏览量，每5秒钟更新一次；另外为了更加清晰地展示，
 * 还应该把窗口的起始结束时间一起输出。我们可以定义滑动窗口，
 * 并结合增量聚合函数和全窗口函数来得到统计结果。
 */
public class Flink13_ReduceOrAggregatorProcessWindowFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> ds = Flink06_EventSource.getEventSource(env);

        ds.print("input");
        SingleOutputStreamOperator<Event> ds1 = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((ev, ts) -> ev.getTs()));


        // 增量聚合完成每个url点击次数的统计
        // 全窗口函数获取窗口的开始和结束时间

        ds1.map(e -> new WordCount(e.getUrl(), 1))
                .keyBy(WordCount::getWord)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<WordCount, Long, WordCount>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(WordCount value, Long accumulator) {
                        return accumulator + value.getCount();
                    }

                    @Override
                    public WordCount getResult(Long accumulator) {
                        WordCount wordCount = new WordCount();
                        wordCount.setCount(accumulator.intValue());
                        return wordCount;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }, new ProcessWindowFunction<WordCount, UrlViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WordCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<WordCount> elements, Collector<UrlViewCount> out) throws Exception {
                        // 通过增量聚合处理的数据，传入到全窗口只会有一条数据
                        WordCount wordCount = elements.iterator().next();
                        // 补充key
                        //补充窗口信息
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect(new UrlViewCount(s,wordCount.getCount().longValue(),start,end));
                    }
                }).print("window");
        env.execute();
    }


}
