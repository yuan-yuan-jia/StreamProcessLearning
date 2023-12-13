package com.atguigu.flink.state;

import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import com.atguigu.flink.source.Flink06_EventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 网站中一个非常经典的例子，就是实时统计一段时间内的热门url。
 * 例如，需要统计最近10秒钟内最热门的两个url链接，
 * 并且每5秒钟更新一次。我们知道，这可以用一个滑动窗口来实现，
 * 而“热门度”一般可以直接用访问量来表示。
 * 于是就需要开滑动窗口收集url的访问数据，
 * 按照不同的url进行统计，而后汇总排序并最终输出前两名。这其实就是著名的“Top N”问题。
 * <p>
 * 1. 基于增量聚合完成url点击次数统计,同时基于全窗口函数，获取窗口信息
 * 2. 按照窗口的结束时间keyby，再以窗口的结束时间注册定时器
 * 3. 定时器触发，数据全部收集，对收集的数据进行topN
 */
public class Flink13_TOPN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = Flink06_EventSource.getEventSource(env)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((e, l) -> e.getTs())
                );

        ds.print("input");

        SingleOutputStreamOperator<UrlViewCount> windowDs = ds.map(e -> new WordCount(e.getUrl(), 1))
                .keyBy(WordCount::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<WordCount, UrlViewCount, UrlViewCount>() {
                    @Override
                    public UrlViewCount createAccumulator() {
                        return new UrlViewCount(null, 0L, null, null);
                    }

                    @Override
                    public UrlViewCount add(WordCount value, UrlViewCount accumulator) {
                        accumulator.setCount(accumulator.getCount() + value.getCount());
                        return accumulator;
                    }

                    @Override
                    public UrlViewCount getResult(UrlViewCount accumulator) {
                        return accumulator;
                    }

                    @Override
                    public UrlViewCount merge(UrlViewCount a, UrlViewCount b) {
                        return null;
                    }
                }, new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> elements, Collector<UrlViewCount> out) throws Exception {
                        UrlViewCount next = elements.iterator().next();

                        next.setUrl(s);
                        next.setWindowStart(context.window().getStart());
                        next.setWindowEnd(context.window().getEnd());
                        out.collect(next);
                    }
                });

        windowDs.print("window >>>>");
        // 处理window输出的数据
        windowDs.keyBy(UrlViewCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, UrlViewCount, String>() {

                    private ListState<UrlViewCount> listState;

                    private ValueState<Boolean> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("urlview", UrlViewCount.class));
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("valueS", Types.BOOLEAN));
                    }

                    @Override
                    public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
                        // 先将数据维护到状态中
                        listState.add(value);

                        // 注册定时器
                        if (valueState.value() == null) {
                            ctx.timerService().registerEventTimeTimer(
                                  ctx.getCurrentKey()
                            );
                            valueState.update(true);
                        }
                    }

                    /*
                    定时器触发，完成topN的处理
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        //从状态中取出所有的数据
                        ArrayList<UrlViewCount> urlViewCounts = new ArrayList<>();
                        for (UrlViewCount urlViewCount : listState.get()) {
                            urlViewCounts.add(urlViewCount);
                        }
                        urlViewCounts.sort(new Comparator<UrlViewCount>() {
                            @Override
                            public int compare(UrlViewCount o1, UrlViewCount o2) {
                                return Long.compare(o2.getCount(),o1.getCount());
                            }
                        });

                        StringBuilder sb = new StringBuilder("***************");

                        // 去tobN
                        for (int i = 0; i < 2 && i < urlViewCounts.size(); i++) {
                            sb.append("Top." +(i+1)+urlViewCounts.get(i) + "\n");
                        }
                        sb.append("*******************");
                        out.collect(sb.toString());

                        listState.clear();
                        valueState.clear();


                    }
                }).print();


        env.execute();

    }
}
