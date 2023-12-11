package com.atguigu.flink.window;

import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 迟到数据的处理
 * 1. 推迟水位线的推进，可以解决正常迟到的数据(绝大部分)
 * 2. 窗口延迟关闭，可以解决迟到更久的数据(少量数据)
 * 3. 侧输出流捕获迟到数据 可以解决极端的数据(个别数据)
 */
public class Flink14_LateData {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2])
                    );
                });

        ds.print("input");


        OutputTag<WordCount> lateOutputTag = new OutputTag<>("late", Types.POJO(WordCount.class));


        // 加上水位线推迟2秒
        SingleOutputStreamOperator<UrlViewCount> ds4 = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((e, l) -> e.getTs())
                ).map(e -> new WordCount(e.getUrl(), 1))
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount value) throws Exception {
                        return value.getWord();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateOutputTag)
                .allowedLateness(Time.seconds(5))
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

        ds4.print("window");
        ds4.getSideOutput(lateOutputTag).print("late");


        env.execute();
    }
}
